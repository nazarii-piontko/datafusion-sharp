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

    protected override bool ReleaseHandle()
    {
        return NativeMethods.RuntimeShutdown(handle) == DataFusionErrorCode.Ok;
    }
}
