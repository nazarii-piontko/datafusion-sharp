using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace DataFusionSharp.Interop;

internal static class AsyncOperationCallbacks
{
    public static void VoidResult(IntPtr _, IntPtr error, ulong handle)
    {
        var exception = error != IntPtr.Zero ? ErrorInfoData.FromIntPtr(error).ToException() : null;
        AsyncOperations.Instance.CompleteVoid(handle, exception);
    }
    
    public static void IntPtrResult(IntPtr result, IntPtr error, ulong handle)
    {
        if (error == IntPtr.Zero)
            AsyncOperations.Instance.CompleteWithResult(handle, Marshal.ReadIntPtr(result));
        else
            AsyncOperations.Instance.CompleteWithError<IntPtr>(handle, ErrorInfoData.FromIntPtr(error).ToException());
    }
    
    public static void UInt64Result(IntPtr result, IntPtr error, ulong handle)
    {
        if (error == IntPtr.Zero)
            AsyncOperations.Instance.CompleteWithResult(handle, (ulong)Marshal.ReadInt64(result));
        else
            AsyncOperations.Instance.CompleteWithError<ulong>(handle, ErrorInfoData.FromIntPtr(error).ToException());
    }
    
    public static void SchemaResult(IntPtr result, IntPtr error, ulong handle)
    {
        if (error == IntPtr.Zero)
        {
            try
            {
                var data = BytesData.FromIntPtr(result);

                using var nativeMemoryManager = new NativeMemoryManager(data.DataPtr, data.Length);
                using var reader = new ArrowStreamReader(nativeMemoryManager.Memory);
                AsyncOperations.Instance.CompleteWithResult(handle, reader.Schema);
            }
            catch (Exception ex)
            {
                AsyncOperations.Instance.CompleteWithError<Schema>(handle, ex);
            }
        }
        else
            AsyncOperations.Instance.CompleteWithError<IntPtr>(handle, ErrorInfoData.FromIntPtr(error).ToException());
    }
}