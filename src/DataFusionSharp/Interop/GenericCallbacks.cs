using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace DataFusionSharp.Interop;

internal static class GenericCallbacks
{
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void CallbackForVoid(IntPtr _, IntPtr error, IntPtr handle)
    {
        var ex = error != IntPtr.Zero ? ErrorInfoData.FromIntPtr(error).ToException() : null;
        var op = AsyncVoidOperation.FromHandle(handle);
        op?.Complete(ex);
    }
    
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void CallbackForVoidSync(IntPtr _, IntPtr error, IntPtr handle)
    {
        var ex = error != IntPtr.Zero ? ErrorInfoData.FromIntPtr(error).ToException() : null;
        var op = SyncVoidOperation.FromHandle(handle);
        op?.Complete(ex);
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void CallbackForString(IntPtr result, IntPtr error, IntPtr handle)
    {
        var op = AsyncOperation<string>.FromHandle(handle);
        if (op is null)
            return;

        if (error != IntPtr.Zero)
        {
            var ex = ErrorInfoData.FromIntPtr(error).ToException();
            op.Complete(ex);
            return;
        }

        var data = BytesData.FromIntPtr(result);
        var dataStr = data.ToUtf8String();
        op.Complete(dataStr);
    }
    
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void CallbackForBytes(IntPtr result, IntPtr error, IntPtr handle)
    {
        var op = AsyncOperation<byte[]>.FromHandle(handle);
        if (op is null)
            return;

        if (error != IntPtr.Zero)
        {
            var ex = ErrorInfoData.FromIntPtr(error).ToException();
            op.Complete(ex);
            return;
        }

        var data = BytesData.FromIntPtr(result);
        var dataBytes = data.ToArray();
        op.Complete(dataBytes);
    }
}