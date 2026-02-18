using System.Buffers;
using Google.Protobuf;

namespace DataFusionSharp.Interop;

/// <summary>
/// A helper struct that pins the byte array of a protobuf message in memory and provides a way to convert it to a BytesData struct for interop.
/// </summary>
internal readonly ref struct PinnedProtobufData : IDisposable
{
    private readonly MemoryHandle? _handle;
    private readonly int _length;

    private PinnedProtobufData(MemoryHandle? message, int length)
    {
        _handle = message;
        _length = length;
    }

    public BytesData ToBytesData()
    {
        return _handle == null
            ? BytesData.Empty
            : BytesData.FromPinned(_handle.Value, _length);
    }
    
    public void Dispose()
    {
        _handle?.Dispose();
    }
    
    public static PinnedProtobufData FromMessage<TMessage>(TMessage? message)
        where TMessage : class, IMessage<TMessage>
    {
        if (message is null)
            return new PinnedProtobufData(null, 0);
        
        var bytes = message.ToByteArray();
        var handle = bytes.AsMemory().Pin();
        return new PinnedProtobufData(handle, bytes.Length);
    }
}