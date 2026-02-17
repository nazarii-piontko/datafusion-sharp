using System.Buffers;
using System.Runtime.InteropServices;

namespace DataFusionSharp.Interop;

[StructLayout(LayoutKind.Sequential)]
internal struct BytesData
{
    internal static BytesData Empty = new()
    {
        DataPtr = 0,
        Length = 0
    };
    
    /// <summary>
    /// Data pointer, UTF-8 encoded, *const u8
    /// </summary>
    public nint DataPtr;
    
    /// <summary>
    /// Data length, u32
    /// </summary>
    public int Length;
    
    /// <summary>
    /// Interpret bytes as a managed string.
    /// </summary>
    public readonly string GetAsUtf8()
    {
        if (DataPtr == IntPtr.Zero || Length == 0)
            return string.Empty;
            
        var message = Marshal.PtrToStringUTF8(DataPtr, Length);
        return message;
    }
    
    /// <summary>
    /// Interpret bytes as a managed byte array.
    /// </summary>
    public readonly byte[] ToArray()
    {
        if (DataPtr == 0 || Length <= 0)
            return [];

        var arr = new byte[Length];
        Marshal.Copy(DataPtr, arr, 0, Length);
        return arr;
    }
    
    public static BytesData FromIntPtr(IntPtr ptr)
    {
        var data = Marshal.PtrToStructure<BytesData>(ptr);
        return data;
    }
    
    public static BytesData FromPinned(MemoryHandle handle, int length)
    {
        BytesData data = new();
        
        unsafe
        {
            data.DataPtr = (nint)handle.Pointer;
        }

        data.Length = length;

        return data;
    }
}