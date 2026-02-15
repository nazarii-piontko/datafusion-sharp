using System.Runtime.InteropServices;

namespace DataFusionSharp.Interop;

[StructLayout(LayoutKind.Sequential)]
internal struct BytesData
{
    /// <summary>
    /// String data pointer, UTF-8 encoded, *const u8
    /// </summary>
    public IntPtr DataPtr;
    
    /// <summary>
    /// String data length, u32
    /// </summary>
    public int Length;

    public BytesData()
    {
    }
    
    public BytesData(IntPtr dataPtr, int length)
    {
        DataPtr = dataPtr;
        Length = length;
    }
    
    public unsafe BytesData(byte* ptr, int length)
    {
        DataPtr = new IntPtr(ptr);
        Length = length;
    }
    
    /// <summary>
    /// Gets the message as a managed string.
    /// </summary>
    /// <returns></returns>
    public string GetAsUtf8()
    {
        if (DataPtr == IntPtr.Zero || Length == 0)
            return string.Empty;
            
        var message = Marshal.PtrToStringUTF8(DataPtr, Length);
        return message;
    }
    
    public static BytesData FromIntPtr(IntPtr ptr)
    {
        var data = Marshal.PtrToStructure<BytesData>(ptr);
        return data;
    }
}