using System.Buffers;
using System.Runtime.InteropServices;

namespace DataFusionSharp.Interop;

[StructLayout(LayoutKind.Sequential)]
internal struct BytesData
{
    /// <summary>
    /// An empty BytesData instance with a null data pointer and zero length.
    /// </summary>
    internal static readonly BytesData Empty = new()
    {
        DataPtr = IntPtr.Zero,
        Length = 0
    };
    
    /// <summary>
    /// Data pointer, UTF-8 encoded, *const u8
    /// </summary>
    public IntPtr DataPtr;
    
    /// <summary>
    /// Data length, u32
    /// </summary>
    public int Length;
    
    /// <summary>
    /// Interpret bytes as a managed string.
    /// </summary>
    public readonly string ToUtf8String()
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
    
    /// <summary>
    /// Create a BytesData from an unmanaged pointer.
    /// The caller is responsible for ensuring the pointer is valid and remains valid for the lifetime of the BytesData.
    /// </summary>
    /// <param name="ptr">Pointer to the unmanaged data.</param>
    public static BytesData FromIntPtr(IntPtr ptr)
    {
        // Manually read the struct fields from the pointer since Marshal.PtrToStructure is slow,
        // and we want to avoid unnecessary copying of the data.
        var data = new BytesData
        {
            DataPtr = Marshal.ReadIntPtr(ptr, 0),
            Length = Marshal.ReadInt32(ptr, IntPtr.Size)
        };
        return data;
    }
    
    /// <summary>
    /// Create a BytesData from a pinned memory handle.
    /// </summary>
    /// <param name="handle">Memory handle containing the pinned data.</param>
    /// <param name="length">Length of the data in bytes.</param>
    public static BytesData FromPinned(MemoryHandle handle, int length)
    {
        BytesData data = new();
        
        unsafe
        {
            data.DataPtr = (IntPtr)handle.Pointer;
        }

        data.Length = length;

        return data;
    }
}