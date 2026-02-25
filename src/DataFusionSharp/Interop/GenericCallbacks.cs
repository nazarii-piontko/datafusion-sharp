namespace DataFusionSharp.Interop;

internal static partial class GenericCallbacks
{
    [DataFusionSharpNativeCallback]
    internal static void CallbackForVoid(IntPtr _, IntPtr error, ulong handle)
    {
        var ex = error != IntPtr.Zero ? ErrorInfoData.FromIntPtr(error).ToException() : null;
        AsyncOperations.Instance.CompleteVoid(handle, ex);
    }

    [DataFusionSharpNativeCallback]
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
}