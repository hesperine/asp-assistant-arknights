using ASPAssistant.App.Windows;
using FluentAssertions;

namespace ASPAssistant.Tests.Services;

public sealed class ConnectionTestCoordinatorTests
{
    private static readonly TimeSpan SafetyTimeout = TimeSpan.FromSeconds(5);
    private static TaskCompletionSource Signal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private sealed class TestResource(bool blockDisposal = false) : IDisposable
    {
        private int _disposeCount;
        internal readonly TaskCompletionSource DisposalStarted = Signal();
        internal readonly TaskCompletionSource AllowDisposal = Signal();
        internal readonly TaskCompletionSource Disposed = Signal();
        internal int DisposeCount => Volatile.Read(ref _disposeCount);

        public void Dispose()
        {
            Interlocked.Increment(ref _disposeCount);
            DisposalStarted.TrySetResult();
            if (blockDisposal) AllowDisposal.Task.WaitAsync(SafetyTimeout).GetAwaiter().GetResult();
            Disposed.TrySetResult();
        }
    }

    private static ConnectionTestCoordinator NewCoordinator(TaskCompletionSource deadline)
    {
        var runs = 0;
        return new((_, token) => Interlocked.Increment(ref runs) == 1
            ? deadline.Task.WaitAsync(token) : Task.Delay(Timeout.InfiniteTimeSpan, token));
    }

    private static Task<ConnectionTestCoordinator.Result<TestResource, int>> Run(
        ConnectionTestCoordinator coordinator, Func<TestResource> create, Func<TestResource, int> test)
        => coordinator.RunAsync(create, test, TimeSpan.FromSeconds(60));

    private static async Task<ConnectionTestCoordinator.Result<TestResource, int>> WhenAvailable(
        ConnectionTestCoordinator coordinator)
    {
        using var timeout = new CancellationTokenSource(SafetyTimeout);
        while (true)
        {
            var result = await Run(coordinator, () => new TestResource(), _ => 7).WaitAsync(timeout.Token);
            if (result.Status != ConnectionTestStatus.Busy) return result;
            await Task.Delay(5, timeout.Token);
        }
    }

    private static TestResource Adopt(ConnectionTestCoordinator.Result<TestResource, int> result)
    {
        result.Status.Should().Be(ConnectionTestStatus.Succeeded);
        using var lease = result.Success!;
        return lease.Adopt();
    }

    [Fact]
    public async Task SuccessfulResultTransfersOwnershipAndReleasesSlot()
    {
        var coordinator = NewCoordinator(Signal());
        var resource = new TestResource();
        var result = await Run(coordinator, () => resource, _ => 42).WaitAsync(SafetyTimeout);

        result.Status.Should().Be(ConnectionTestStatus.Succeeded);
        result.Success!.Value.Should().Be(42);
        (await Run(coordinator, () => new TestResource(), _ => 1)).Status.Should().Be(ConnectionTestStatus.Busy);
        Adopt(result).Should().BeSameAs(resource);
        resource.DisposeCount.Should().Be(0);

        var next = await Run(coordinator, () => new TestResource(), _ => 7).WaitAsync(SafetyTimeout);
        next.Status.Should().Be(ConnectionTestStatus.Succeeded);
        Adopt(next).Dispose();
        resource.Dispose();
        resource.DisposeCount.Should().Be(1);
    }

    [Fact]
    public async Task FailureReturnsWithoutWaitingForDisposalAndKeepsSlotUntilCleanup()
    {
        var coordinator = NewCoordinator(Signal());
        var resource = new TestResource(blockDisposal: true);
        var failure = new IOException("capture failed");
        try
        {
            var result = await Run(coordinator, () => resource, _ => throw failure).WaitAsync(SafetyTimeout);
            result.Status.Should().Be(ConnectionTestStatus.Failed);
            result.Error.Should().BeSameAs(failure);
            await resource.DisposalStarted.Task.WaitAsync(SafetyTimeout);
            (await Run(coordinator, () => new TestResource(), _ => 1)).Status.Should().Be(ConnectionTestStatus.Busy);
        }
        finally { resource.AllowDisposal.TrySetResult(); }

        await resource.Disposed.Task.WaitAsync(SafetyTimeout);
        var next = await WhenAvailable(coordinator);
        next.Status.Should().Be(ConnectionTestStatus.Succeeded);
        Adopt(next).Dispose();
        resource.DisposeCount.Should().Be(1);
    }

    [Fact]
    public async Task TimeoutReturnsWhileWorkIsBlockedAndSlotRemainsHeldThroughDisposal()
    {
        var deadline = Signal();
        var coordinator = NewCoordinator(deadline);
        var resource = new TestResource(blockDisposal: true);
        var started = Signal();
        var finishWork = Signal();
        var created = 0;
        try
        {
            var run = Run(coordinator, () => resource, _ =>
            {
                started.TrySetResult();
                finishWork.Task.WaitAsync(SafetyTimeout).GetAwaiter().GetResult();
                return 42;
            });
            await started.Task.WaitAsync(SafetyTimeout);
            deadline.SetResult();
            (await run.WaitAsync(SafetyTimeout)).Status.Should().Be(ConnectionTestStatus.TimedOut);
            resource.DisposeCount.Should().Be(0);
            var rejected = await Run(coordinator, () => { created++; return new TestResource(); }, _ => 1);
            rejected.Status.Should().Be(ConnectionTestStatus.Busy);
            created.Should().Be(0);

            finishWork.SetResult();
            await resource.DisposalStarted.Task.WaitAsync(SafetyTimeout);
            (await Run(coordinator, () => new TestResource(), _ => 1)).Status.Should().Be(ConnectionTestStatus.Busy);
        }
        finally
        {
            finishWork.TrySetResult();
            resource.AllowDisposal.TrySetResult();
        }

        await resource.Disposed.Task.WaitAsync(SafetyTimeout);
        var next = await WhenAvailable(coordinator);
        next.Status.Should().Be(ConnectionTestStatus.Succeeded);
        Adopt(next).Dispose();
        resource.DisposeCount.Should().Be(1);
    }

    [Fact]
    public async Task DiscardingUnadoptedSuccessDisposesExactlyOnce()
    {
        var coordinator = NewCoordinator(Signal());
        var resource = new TestResource(blockDisposal: true);
        try
        {
            var result = await Run(coordinator, () => resource, _ => 42).WaitAsync(SafetyTimeout);
            result.Status.Should().Be(ConnectionTestStatus.Succeeded);
            using (result.Success!) { }
            result.Success!.Dispose(); // Discarding twice cannot dispose the resource twice.
            await resource.DisposalStarted.Task.WaitAsync(SafetyTimeout);
            (await Run(coordinator, () => new TestResource(), _ => 1)).Status.Should().Be(ConnectionTestStatus.Busy);
        }
        finally { resource.AllowDisposal.TrySetResult(); }

        await resource.Disposed.Task.WaitAsync(SafetyTimeout);
        var next = await WhenAvailable(coordinator);
        Adopt(next).Dispose();
        resource.DisposeCount.Should().Be(1);
    }

    [Fact]
    public async Task RetiredSuccessfulConnectionWaitsBehindActiveTestWithoutLettingNewTestsStart()
    {
        var coordinator = NewCoordinator(Signal());
        var retired = new TestResource(blockDisposal: true);
        var first = await Run(coordinator, () => retired, _ => 1).WaitAsync(SafetyTimeout);
        Adopt(first);
        var started = Signal();
        var finishWork = Signal();
        try
        {
            var current = Run(coordinator, () => new TestResource(), _ =>
            {
                started.TrySetResult();
                finishWork.Task.WaitAsync(SafetyTimeout).GetAwaiter().GetResult();
                return 2;
            });
            await started.Task.WaitAsync(SafetyTimeout);
            coordinator.DisposeRetired(retired);
            retired.DisposeCount.Should().Be(0);
            finishWork.SetResult();
            var result = await current.WaitAsync(SafetyTimeout);
            Adopt(result).Dispose();
            await retired.DisposalStarted.Task.WaitAsync(SafetyTimeout);
            (await Run(coordinator, () => new TestResource(), _ => 1)).Status.Should().Be(ConnectionTestStatus.Busy);
        }
        finally
        {
            finishWork.TrySetResult();
            retired.AllowDisposal.TrySetResult();
        }

        await retired.Disposed.Task.WaitAsync(SafetyTimeout);
        var next = await WhenAvailable(coordinator);
        Adopt(next).Dispose();
        retired.DisposeCount.Should().Be(1);
    }
}
