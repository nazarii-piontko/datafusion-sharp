using System.Runtime.InteropServices;

namespace DataFusionSharp.Interop;

internal static partial class AsyncOperationGenericCallbacks
{
    [NativeCallback]
    internal static void VoidResult(IntPtr _, IntPtr error, ulong handle)
    {
        var exception = error != IntPtr.Zero ? ErrorInfoData.FromIntPtr(error).ToException() : null;
        AsyncOperations.Instance.CompleteVoid(handle, exception);
    }

    [NativeCallback]
    internal static void IntPtrResult(IntPtr result, IntPtr error, ulong handle)
    {
        if (error == IntPtr.Zero)
            AsyncOperations.Instance.CompleteWithResult(handle, Marshal.ReadIntPtr(result));
        else
            AsyncOperations.Instance.CompleteWithError<IntPtr>(handle, ErrorInfoData.FromIntPtr(error).ToException());
    }

    [NativeCallback]
    internal static void UInt64Result(IntPtr result, IntPtr error, ulong handle)
    {
        if (error == IntPtr.Zero)
            AsyncOperations.Instance.CompleteWithResult(handle, (ulong)Marshal.ReadInt64(result));
        else
            AsyncOperations.Instance.CompleteWithError<ulong>(handle, ErrorInfoData.FromIntPtr(error).ToException());
    }

    [NativeCallback]
    internal static void StringResult(IntPtr result, IntPtr error, ulong handle)
    {
        if (error == IntPtr.Zero)
        {
            var data = BytesData.FromIntPtr(result);
            AsyncOperations.Instance.CompleteWithResult(handle, data.ToUtf8String());
        }
        else
            AsyncOperations.Instance.CompleteWithError<string>(handle, ErrorInfoData.FromIntPtr(error).ToException());
    }
}
