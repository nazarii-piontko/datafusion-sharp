using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace DataFusionSharp.Interop;

internal static class GenericCallbacks
{
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void CallbackForVoid(IntPtr _, IntPtr error, ulong handle)
    {
        var ex = error != IntPtr.Zero ? ErrorInfoData.FromIntPtr(error).ToException() : null;
        AsyncOperations.Instance.CompleteVoid(handle, ex);
    }
    
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void CallbackForVoidSync(IntPtr _, IntPtr error, ulong handle)
    {
        var ex = error != IntPtr.Zero ? ErrorInfoData.FromIntPtr(error).ToException() : null;
        SyncOperations.Instance.CompleteVoid(handle, ex);
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void CallbackForString(IntPtr result, IntPtr error, ulong handle)
    {
        if (error != IntPtr.Zero)
        {
            var ex = ErrorInfoData.FromIntPtr(error).ToException();
            AsyncOperations.Instance.CompleteWithError<string>(handle, ex);
            return;
        }

        var data = BytesData.FromIntPtr(result);
        var dataStr = data.ToUtf8String();
        AsyncOperations.Instance.CompleteWithResult(handle, dataStr);
    }
    
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void CallbackForBytes(IntPtr result, IntPtr error, ulong handle)
    {
        if (error != IntPtr.Zero)
        {
            var ex = ErrorInfoData.FromIntPtr(error).ToException();
            AsyncOperations.Instance.CompleteWithError<byte[]>(handle, ex);
            return;
        }

        var data = BytesData.FromIntPtr(result);
        var dataBytes = data.ToArray();
        AsyncOperations.Instance.CompleteWithResult(handle, dataBytes);
    }
}