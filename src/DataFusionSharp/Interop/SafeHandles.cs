using System.Runtime.InteropServices;

namespace DataFusionSharp.Interop;

internal abstract class DataFusionSafeHandle : SafeHandle
{
    protected DataFusionSafeHandle(IntPtr handle)
        : base(IntPtr.Zero, true)
    {
        SetHandle(handle);
    }

    public override bool IsInvalid => handle == IntPtr.Zero;
}

internal sealed class RuntimeSafeHandle : DataFusionSafeHandle
{
#if MEMORY_TEST
    private static long _liveInstances;
    internal static long LiveInstances => Interlocked.Read(ref _liveInstances);
#endif

    internal RuntimeSafeHandle(IntPtr handle)
        : base(handle)
    {
#if MEMORY_TEST
        Interlocked.Increment(ref _liveInstances);
#endif
    }
    
    /// <summary>
    /// Shuts down the DataFusion runtime and releases all associated resources.
    /// </summary>
    /// <exception cref="DataFusionException">Thrown when shutdown fails.</exception>
    internal void Shutdown()
    {
        ObjectDisposedException.ThrowIf(IsClosed, this);
        
        var errorCode = ShutdownInternal();
        DataFusionException.ThrowIfError(errorCode, "Failed to shutdown DataFusion runtime");
        
        SetHandleAsInvalid();
        
        Close();
        
    }

    protected override bool ReleaseHandle()
    {
        return ShutdownInternal() == DataFusionErrorCode.Ok;
    }
    
    private DataFusionErrorCode ShutdownInternal()
    {
#if MEMORY_TEST
        Interlocked.Decrement(ref _liveInstances);
#endif

        return NativeMethods.RuntimeDestroy(handle);
    }
}

internal sealed class SessionContextSafeHandle : DataFusionSafeHandle
{
#if MEMORY_TEST
    private static long _liveInstances;
    internal static long LiveInstances => Interlocked.Read(ref _liveInstances);
#endif

    internal SessionContextSafeHandle(IntPtr handle)
        : base(handle)
    {
#if MEMORY_TEST
        Interlocked.Increment(ref _liveInstances);
#endif
    }

    protected override bool ReleaseHandle()
    {
#if MEMORY_TEST
        Interlocked.Decrement(ref _liveInstances);
#endif

        return NativeMethods.ContextDestroy(handle) == DataFusionErrorCode.Ok;
    }
}

internal sealed class DataFrameSafeHandle : DataFusionSafeHandle
{
#if MEMORY_TEST
    private static long _liveInstances;
    internal static long LiveInstances => Interlocked.Read(ref _liveInstances);
#endif

    internal DataFrameSafeHandle(IntPtr handle)
        : base(handle)
    {
#if MEMORY_TEST
        Interlocked.Increment(ref _liveInstances);
#endif
    }

    protected override bool ReleaseHandle()
    {
#if MEMORY_TEST
        Interlocked.Decrement(ref _liveInstances);
#endif

        return NativeMethods.DataFrameDestroy(handle) == DataFusionErrorCode.Ok;
    }
}

internal sealed class DataFrameStreamSafeHandle : DataFusionSafeHandle
{
#if MEMORY_TEST
    private static long _liveInstances;
    internal static long LiveInstances => Interlocked.Read(ref _liveInstances);
#endif

    internal DataFrameStreamSafeHandle(IntPtr handle)
        : base(handle)
    {
#if MEMORY_TEST
        Interlocked.Increment(ref _liveInstances);
#endif
    }

    protected override bool ReleaseHandle()
    {
#if MEMORY_TEST
        Interlocked.Decrement(ref _liveInstances);
#endif

        return NativeMethods.DataFrameStreamDestroy(handle) == DataFusionErrorCode.Ok;
    }
}

internal sealed class InMemoryStoreSafeHandle : DataFusionSafeHandle
{
#if MEMORY_TEST
    private static long _liveInstances;
    internal static long LiveInstances => Interlocked.Read(ref _liveInstances);
#endif

    internal InMemoryStoreSafeHandle(IntPtr handle)
        : base(handle)
    {
#if MEMORY_TEST
        Interlocked.Increment(ref _liveInstances);
#endif
    }

    protected override bool ReleaseHandle()
    {
#if MEMORY_TEST
        Interlocked.Decrement(ref _liveInstances);
#endif

        return NativeMethods.InMemoryStoreDestroy(handle) == DataFusionErrorCode.Ok;
    }
}
