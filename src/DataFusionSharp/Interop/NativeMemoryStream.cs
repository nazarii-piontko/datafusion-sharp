namespace DataFusionSharp.Interop;

/// <summary>
/// A stream that reads from native memory without copying data to managed memory.
/// 
/// This stream is designed to work with native buffer reads where each "next" 
/// operation from native code returns a buffer. The stream maintains position across 
/// buffer boundaries by updating the underlying memory manager with each new buffer while 
/// tracking cumulative position.
/// </summary>
internal sealed class NativeMemoryStream : Stream
{
    private NativeMemoryManager? _nativeMemoryManager;
    private int _position;
    private int _basePosition;

    internal void SetNativeMemory(NativeMemoryManager nativeMemoryManager)
    {
        _nativeMemoryManager = nativeMemoryManager;
        _basePosition += _position;
        _position = 0;
    }

    public override void Flush()
    {
        throw new NotSupportedException("Native memory does not support writing.");
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        var span = _nativeMemoryManager!.GetSpan();
        if (_position >= span.Length)
            return 0;

        var bytesToRead = Math.Min(count, span.Length - _position);
        
        var sourceSpan = span.Slice(_position, bytesToRead);
        var destinationSpan = buffer.AsSpan(offset, bytesToRead);
        
        sourceSpan.CopyTo(destinationSpan);
        _position += bytesToRead;
        
        return bytesToRead;
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotSupportedException("Native memory does not support seeking.");
    }

    public override void SetLength(long value)
    {
        throw new NotSupportedException("Native memory does not support writing.");
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException("Native memory does not support writing.");
    }

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => _basePosition + _nativeMemoryManager!.Memory.Length;

    public override long Position
    {
        get => _basePosition + _position;
        set => throw new NotSupportedException("Native memory does not support seeking.");
    }
}