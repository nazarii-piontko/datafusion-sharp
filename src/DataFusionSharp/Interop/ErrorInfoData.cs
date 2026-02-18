using System.Runtime.InteropServices;

namespace DataFusionSharp.Interop;

[StructLayout(LayoutKind.Sequential)]
internal struct ErrorInfoData
{
    /// <summary>
    /// Error code
    /// </summary>
    public DataFusionErrorCode Code;
    
    /// <summary>
    /// Error message
    /// </summary>
    public BytesData Message;
    
    public static ErrorInfoData FromIntPtr(IntPtr ptr)
    {
        var data = Marshal.PtrToStructure<ErrorInfoData>(ptr);
        return data;
    }
    
    public Exception ToException()
    {
        var message = Message.ToUtf8String();
        return new DataFusionException(Code, message);
    }
}