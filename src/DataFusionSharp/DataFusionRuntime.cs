using DataFusionSharp.Interop;
using DataFusionSharp.ObjectStore;

namespace DataFusionSharp;

/// <summary>
/// Entry point for the DataFusion library. Manages the underlying async runtime that executes all DataFusion operations.
/// </summary>
/// <remarks>
/// This class wraps a Tokio runtime, which is the Rust equivalent of a thread pool with async task scheduling
/// (similar to <see cref="System.Threading.ThreadPool"/> combined with <see cref="System.Threading.Tasks.TaskScheduler"/>).
/// Typically, only one instance of this class is needed per application. The runtime is thread-safe and can be
/// shared across multiple threads. Create multiple <see cref="SessionContext"/> instances from a single runtime
/// for concurrent query execution.
/// </remarks>
public sealed class DataFusionRuntime : IDisposable
{
    private readonly RuntimeSafeHandle _handle;

    private DataFusionRuntime(RuntimeSafeHandle handle)
    {
        _handle = handle;
    }

    /// <summary>
    /// Creates a new DataFusion runtime with optional thread pool configuration.
    /// </summary>
    /// <param name="workerThreads">Number of worker threads. If null, uses Tokio defaults.</param>
    /// <param name="maxBlockingThreads">Maximum number of blocking threads. If null, uses Tokio defaults.</param>
    /// <returns>A new <see cref="DataFusionRuntime"/> instance.</returns>
    /// <exception cref="DataFusionException">Thrown when runtime creation fails.</exception>
    public static DataFusionRuntime Create(uint? workerThreads = null, uint? maxBlockingThreads = null)
    {
        if (workerThreads.HasValue)
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(workerThreads.Value);
        if (maxBlockingThreads.HasValue)
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxBlockingThreads.Value);
        
        var result = NativeMethods.RuntimeNew(workerThreads ?? 0, maxBlockingThreads ?? 0, out var handle);
        DataFusionException.ThrowIfError(result, "Failed to create DataFusion runtime");

        return new DataFusionRuntime(new RuntimeSafeHandle(handle));
    }

    /// <summary>
    /// Pings the runtime to check if it is responsive.
    /// This is used for testing purposes.
    /// </summary>
    /// <param name="timeout">Operation timeout.</param>
    /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="DataFusionException">Thrown when the ping operation fails.</exception>
    internal Task PingAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        var (id, tcs) = AsyncOperations.Instance.Create(cancellationToken);
        var result = NativeMethods.Ping(_handle, (ulong) timeout.TotalMilliseconds, GenericCallbacks.CallbackForVoidHandle, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to send ping to DataFusion runtime");
        }
        return tcs.Task;
    }
    
    /// <summary>
    /// Creates a new session context for executing queries.
    /// </summary>
    /// <returns>A new <see cref="SessionContext"/> instance.</returns>
    /// <exception cref="DataFusionException">Thrown when context creation fails.</exception>
    public SessionContext CreateSessionContext()
    {
        var errorCode = NativeMethods.ContextNew(_handle, out var contextHandle);
        DataFusionException.ThrowIfError(errorCode, "Failed to create DataFusion context");
        
        return new SessionContext(this, new SessionContextSafeHandle(contextHandle));
    }

    /// <summary>
    /// Creates a new in-memory object store.
    /// </summary>
    /// <returns>A new <see cref="InMemoryObjectStore"/> instance.</returns>
    /// <exception cref="DataFusionException">Thrown when object store creation fails.</exception>
    public InMemoryObjectStore CreateInMemoryStore()
    {
        var errorCode = NativeMethods.InMemoryStoreNew(_handle, out var storeHandle);
        DataFusionException.ThrowIfError(errorCode, "Failed to create in-memory object store");
        
        return new InMemoryObjectStore(this, new InMemoryStoreSafeHandle(storeHandle));
    }
    
    /// <summary>
    /// Shuts down the runtime and releases all resources.
    /// After calling this method, the runtime cannot be used to create new session contexts or execute queries.
    /// </summary>
    /// <remarks>
    /// It is not necessary to call this method explicitly, as the runtime will be automatically shut down when the instance is disposed.
    /// </remarks>
    /// <exception cref="DataFusionException">Thrown when shutdown fails.</exception>
    public void Shutdown()
    {
        _handle.Shutdown();
    }
    
    /// <inheritdoc />
    public void Dispose()
    {
        _handle.Dispose();
    }
}