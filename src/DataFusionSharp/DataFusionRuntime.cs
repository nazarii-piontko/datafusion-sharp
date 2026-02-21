using DataFusionSharp.Interop;

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
    private IntPtr _handle;

    private DataFusionRuntime(IntPtr handle)
    {
        _handle = handle;
    }
    
    /// <summary>
    /// Releases unmanaged resources if <see cref="Dispose"/> was not called.
    /// </summary>
    ~DataFusionRuntime()
    {
        ShutdownRuntime();
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
        if (result != DataFusionErrorCode.Ok)
            throw new DataFusionException(result, "Failed to create DataFusion runtime");

        return new DataFusionRuntime(handle);
    }
    
    /// <summary>
    /// Creates a new session context for executing queries.
    /// </summary>
    /// <returns>A new <see cref="SessionContext"/> instance.</returns>
    /// <exception cref="DataFusionException">Thrown when context creation fails.</exception>
    public SessionContext CreateSessionContext()
    {
        var result = NativeMethods.ContextNew(_handle, out var contextHandle);
        if (result != DataFusionErrorCode.Ok)
            throw new DataFusionException(result, "Failed to create DataFusion session context");
        
        return new SessionContext(this, contextHandle);
    }
    
    /// <summary>
    /// Shuts down the runtime and releases all resources.
    /// </summary>
    public void Dispose()
    {
        ShutdownRuntime();
        GC.SuppressFinalize(this);
    }

    private void ShutdownRuntime()
    {
        var handle = _handle;
        if (handle == IntPtr.Zero)
            return;
        
        _handle = IntPtr.Zero;
        
        NativeMethods.RuntimeShutdown(handle);
    }
}