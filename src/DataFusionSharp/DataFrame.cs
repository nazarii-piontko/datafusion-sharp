using DataFusionSharp.Interop;

namespace DataFusionSharp;

public sealed class DataFrame : IDisposable
{
    private IntPtr _handle;
    private readonly SessionContext _context;
    private bool _disposed;

    internal DataFrame(IntPtr handle, SessionContext context)
    {
        _handle = handle;
        _context = context;
    }

    public ulong Count()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var result = NativeMethods.DataFrameCount(_context.Handle, _handle, out var count);

        if (result != ErrorCode.Ok)
            throw new DataFusionException(_context.GetLastError() ?? $"Failed to count rows: {result}");

        // Count consumes the dataframe in Rust
        _handle = IntPtr.Zero;
        _disposed = true;

        return count;
    }

    public void Show()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var result = NativeMethods.DataFrameShow(_context.Handle, _handle);

        if (result != ErrorCode.Ok)
            throw new DataFusionException(_context.GetLastError() ?? $"Failed to show dataframe: {result}");
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        if (_handle != IntPtr.Zero)
        {
            NativeMethods.DataFrameFree(_handle);
            _handle = IntPtr.Zero;
        }

        _disposed = true;
    }
}
