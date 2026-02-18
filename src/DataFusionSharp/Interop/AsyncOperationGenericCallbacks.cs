using System.Runtime.InteropServices;

namespace DataFusionSharp.Interop;

internal static class AsyncOperationGenericCallbacks
{
    private static void VoidResult(IntPtr _, IntPtr error, ulong handle)
    {
        var exception = error != IntPtr.Zero ? ErrorInfoData.FromIntPtr(error).ToException() : null;
        AsyncOperations.Instance.CompleteVoid(handle, exception);
    }
    private static readonly NativeMethods.Callback VoidResultDelegate = VoidResult;
    public static readonly IntPtr VoidResultHandler = Marshal.GetFunctionPointerForDelegate(VoidResultDelegate);
    
    private static void IntPtrResult(IntPtr result, IntPtr error, ulong handle)
    {
        if (error == IntPtr.Zero)
            AsyncOperations.Instance.CompleteWithResult(handle, Marshal.ReadIntPtr(result));
        else
            AsyncOperations.Instance.CompleteWithError<IntPtr>(handle, ErrorInfoData.FromIntPtr(error).ToException());
    }
    private static readonly NativeMethods.Callback IntPtrResultDelegate = IntPtrResult;
    public static readonly IntPtr IntPtrResultHandler = Marshal.GetFunctionPointerForDelegate(IntPtrResultDelegate);
    
    private static void UInt64Result(IntPtr result, IntPtr error, ulong handle)
    {
        if (error == IntPtr.Zero)
            AsyncOperations.Instance.CompleteWithResult(handle, (ulong)Marshal.ReadInt64(result));
        else
            AsyncOperations.Instance.CompleteWithError<ulong>(handle, ErrorInfoData.FromIntPtr(error).ToException());
    }
    private static readonly NativeMethods.Callback UInt64ResultDelegate = UInt64Result;
    public static readonly IntPtr UInt64ResultHandler = Marshal.GetFunctionPointerForDelegate(UInt64ResultDelegate);

    private static void StringResult(IntPtr result, IntPtr error, ulong handle)
    {
        if (error == IntPtr.Zero)
        {
            var data = BytesData.FromIntPtr(result);
            AsyncOperations.Instance.CompleteWithResult(handle, data.ToUtf8String());
        }
        else
            AsyncOperations.Instance.CompleteWithError<string>(handle, ErrorInfoData.FromIntPtr(error).ToException());
    }
    private static readonly NativeMethods.Callback StringResultDelegate = StringResult;
    public static readonly IntPtr StringResultHandler = Marshal.GetFunctionPointerForDelegate(StringResultDelegate);
}