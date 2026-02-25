using System.Runtime.InteropServices;

namespace DataFusionSharp.Interop;

internal static class AsyncOperationGenericCallbacks
{
    private static void VoidResult(IntPtr _, IntPtr error, ulong handle)
    {
        var ex = error != IntPtr.Zero ? ErrorInfoData.FromIntPtr(error).ToException() : null;
        AsyncOperations.Instance.CompleteVoid(handle, ex);
    }
    private static readonly NativeMethods.Callback VoidResultDelegate = VoidResult;
    public static readonly IntPtr VoidResultHandle = Marshal.GetFunctionPointerForDelegate(VoidResultDelegate);

    private static void StringResult(IntPtr result, IntPtr error, ulong handle)
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
    private static readonly NativeMethods.Callback StringResultDelegate = StringResult;
    public static readonly IntPtr StringResultHandle = Marshal.GetFunctionPointerForDelegate(StringResultDelegate);
}