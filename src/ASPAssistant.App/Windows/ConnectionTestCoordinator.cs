using ASPAssistant.Core;

namespace ASPAssistant.App.Windows;

internal enum ConnectionTestStatus { Succeeded, Failed, TimedOut, Busy }

/// <summary>
/// Owns the one connection test allowed in this process. Abandoned native calls cannot be
/// canceled, so the slot stays occupied until the call and its resource disposal both finish.
/// </summary>
internal sealed class ConnectionTestCoordinator
{
    internal static ConnectionTestCoordinator Shared { get; } = new();

    private readonly object _gate = new();
    private readonly Queue<IDisposable> _retired = new();
    private bool _occupied;
    private readonly Func<TimeSpan, CancellationToken, Task> _delay;

    internal ConnectionTestCoordinator(Func<TimeSpan, CancellationToken, Task>? delay = null)
        => _delay = delay ?? Task.Delay;

    internal sealed record Result<TResource, TValue>(ConnectionTestStatus Status,
        Lease<TResource, TValue>? Success = null, Exception? Error = null) where TResource : class, IDisposable;

    /// <summary>A completed result remains owned by the coordinator until explicitly adopted.</summary>
    internal sealed class Lease<TResource, TValue> : IDisposable where TResource : class, IDisposable
    {
        private readonly ConnectionTestCoordinator _owner;
        private TResource? _resource;

        internal Lease(ConnectionTestCoordinator owner, TResource resource, TValue value)
            => (_owner, _resource, Value) = (owner, resource, value);

        internal TValue Value { get; }

        internal TResource Adopt()
        {
            var resource = Interlocked.Exchange(ref _resource, null)
                ?? throw new InvalidOperationException("The test result has already been adopted or discarded.");
            _owner.ReleaseSlot();
            return resource;
        }

        public void Dispose()
        {
            var resource = Interlocked.Exchange(ref _resource, null);
            if (resource is not null) _owner.DisposeAndRelease(resource);
        }
    }

    private sealed record WorkResult<TResource, TValue>(TResource? Resource, TValue? Value, Exception? Error)
        where TResource : class, IDisposable;

    internal async Task<Result<TResource, TValue>> RunAsync<TResource, TValue>(
        Func<TResource> create, Func<TResource, TValue> test, TimeSpan timeout)
        where TResource : class, IDisposable
    {
        if (!TryOccupy()) return new(ConnectionTestStatus.Busy);

        using var timerCancellation = new CancellationTokenSource();
        Task deadline;
        Task<WorkResult<TResource, TValue>> work;
        try
        {
            deadline = _delay(timeout, timerCancellation.Token);
            work = Task.Run(() =>
            {
                TResource? resource = null;
                try
                {
                    resource = create() ?? throw new InvalidOperationException("The connection test returned no resource.");
                    return new WorkResult<TResource, TValue>(resource, test(resource), null);
                }
                catch (Exception error) { return new WorkResult<TResource, TValue>(resource, default, error); }
            });
        }
        catch (Exception error)
        {
            timerCancellation.Cancel();
            ReleaseSlot();
            return new(ConnectionTestStatus.Failed, Error: error);
        }

        var finished = await Task.WhenAny(work, deadline).ConfigureAwait(false);
        timerCancellation.Cancel();

        if (finished == work)
        {
            var result = await work.ConfigureAwait(false);
            if (result.Error is null)
                return new(ConnectionTestStatus.Succeeded, new Lease<TResource, TValue>(this, result.Resource!, result.Value!));
            DisposeAndRelease(result.Resource);
            return new(ConnectionTestStatus.Failed, Error: result.Error);
        }

        // Run the entire continuation on the thread pool, even when work just completed:
        // Dispose itself can block and must never run on the UI thread.
        DisposeAndReleaseWhenFinished(work);
        try { await deadline.ConfigureAwait(false); }
        catch (Exception error) { return new(ConnectionTestStatus.Failed, Error: error); }
        return new(ConnectionTestStatus.TimedOut);
    }

    /// <summary>Retire an adopted but unused successful connection without blocking the UI.</summary>
    internal void DisposeRetired(IDisposable resource)
    {
        lock (_gate)
        {
            if (_occupied)
            {
                // These resources were adopted before the current operation. No new
                // tests can start until the existing operation and this queue finish.
                _retired.Enqueue(resource);
                return;
            }
            _occupied = true;
        }
        DisposeAndRelease(resource);
    }

    private bool TryOccupy()
    {
        lock (_gate)
        {
            if (_occupied) return false;
            _occupied = true;
            return true;
        }
    }

    private void ReleaseSlot()
    {
        IDisposable? next;
        lock (_gate)
        {
            if (!_retired.TryDequeue(out next))
            {
                _occupied = false;
                return;
            }
        }
        DisposeAndRelease(next);
    }

    private void DisposeAndRelease(IDisposable? resource)
        => _ = Task.Run(() => DisposeAndReleaseCore(resource));

    private void DisposeAndReleaseWhenFinished<TResource, TValue>(Task<WorkResult<TResource, TValue>> work)
        where TResource : class, IDisposable
        => _ = Task.Run(async () =>
        {
            WorkResult<TResource, TValue> result;
            try
            {
                result = await work.ConfigureAwait(false);
            }
            catch (Exception error)
            {
                AppLogger.Warn("ConnectionTest", $"后台连接测试结束失败：{error}");
                ReleaseSlot();
                return;
            }
            DisposeAndReleaseCore(result.Resource);
        });

    private void DisposeAndReleaseCore(IDisposable? resource)
    {
        try { resource?.Dispose(); }
        catch (Exception error) { AppLogger.Warn("ConnectionTest", $"释放测试连接失败：{error}"); }
        finally { ReleaseSlot(); }
    }
}
