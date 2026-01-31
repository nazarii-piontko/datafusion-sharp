using System.Runtime.InteropServices;
using DataFusionSharp.Interop;

namespace DataFusionSharp;

public sealed class SessionContext : IDisposable
{
    private IntPtr _handle;
    private bool _disposed;

    public SessionContext()
    {
        if (!DataFusion.IsInitialized)
            DataFusion.Initialize();

        _handle = NativeMethods.ContextNew();

        if (_handle == IntPtr.Zero)
            throw new DataFusionException("Failed to create SessionContext");
    }

    internal IntPtr Handle
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _handle;
        }
    }

    public void RegisterCsv(string tableName, string path)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        var result = NativeMethods.ContextRegisterCsv(_handle, tableName, path);

        if (result != ErrorCode.Ok)
            throw new DataFusionException(GetLastError() ?? $"Failed to register CSV: {result}");
    }

    public DataFrame Sql(string sql)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);

        var dfHandle = NativeMethods.ContextSql(_handle, sql);

        if (dfHandle == IntPtr.Zero)
            throw new DataFusionException(GetLastError() ?? "Failed to execute SQL");

        return new DataFrame(dfHandle, this);
    }

    internal string? GetLastError()
    {
        var length = NativeMethods.ContextLastErrorLength(_handle);

        if (length <= 0)
            return null;

        var errorPtr = NativeMethods.ContextLastError(_handle);

        if (errorPtr == IntPtr.Zero)
            return null;

        return Marshal.PtrToStringUTF8(errorPtr, length);
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        if (_handle != IntPtr.Zero)
        {
            NativeMethods.ContextFree(_handle);
            _handle = IntPtr.Zero;
        }

        _disposed = true;
    }
}
