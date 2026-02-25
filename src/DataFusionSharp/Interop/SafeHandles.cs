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
    internal RuntimeSafeHandle(IntPtr handle)
        : base(handle)
    {
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
        return NativeMethods.RuntimeDestroy(handle);
    }
}

internal sealed class SessionContextSafeHandle : DataFusionSafeHandle
{
    internal SessionContextSafeHandle(IntPtr handle)
        : base(handle)
    {
    }

    protected override bool ReleaseHandle()
    {
        return NativeMethods.ContextDestroy(handle) == DataFusionErrorCode.Ok;
    }
}

internal sealed class DataFrameSafeHandle : DataFusionSafeHandle
{
    internal DataFrameSafeHandle(IntPtr handle)
        : base(handle)
    {
    }

    protected override bool ReleaseHandle()
    {
        return NativeMethods.DataFrameDestroy(handle) == DataFusionErrorCode.Ok;
    }
}

internal sealed class DataFrameStreamSafeHandle : DataFusionSafeHandle
{
    internal DataFrameStreamSafeHandle(IntPtr handle)
        : base(handle)
    {
    }

    protected override bool ReleaseHandle()
    {
        return NativeMethods.DataFrameStreamDestroy(handle) == DataFusionErrorCode.Ok;
    }
}
