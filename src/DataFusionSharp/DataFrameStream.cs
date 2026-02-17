using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using DataFusionSharp.Interop;

namespace DataFusionSharp;

/// <summary>
/// An async stream of Arrow record batches from a DataFrame query execution.
/// </summary>
/// <remarks>
/// Use this class to process query results incrementally without loading all data into memory.
/// Each iteration yields a <see cref="RecordBatch"/> containing a subset of the result rows.
/// This class is not thread-safe. Do not call methods on the same instance concurrently from multiple threads.
/// </remarks>
#pragma warning disable CA1711
public sealed class DataFrameStream : IAsyncEnumerable<RecordBatch>, IDisposable
#pragma warning restore CA1711
{
    private IntPtr _handle;
    private NativeMemoryStream? _nativeMemoryStream;
    private ArrowStreamReader? _reader;

    /// <summary>
    /// Gets the DataFrame that created this stream.
    /// </summary>
    public DataFrame DataFrame { get; }

    internal DataFrameStream(DataFrame dataFrame, IntPtr handle)
    {
        DataFrame = dataFrame;
        _handle = handle;
    }

    /// <summary>
    /// Releases unmanaged resources if <see cref="Dispose"/> was not called.
    /// </summary>
    ~DataFrameStream()
    {
        DestroyStream();
    }

    /// <summary>
    /// Returns an async enumerator that iterates through the record batches.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the enumeration.</param>
    /// <returns>An async enumerator of <see cref="RecordBatch"/>.</returns>
    public async IAsyncEnumerator<RecordBatch> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        while (await NextAsync().ConfigureAwait(false) is { } batch)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return batch;
        }
    }

    /// <summary>
    /// Releases all resources used by this stream.
    /// </summary>
    public void Dispose()
    {
        _reader?.Dispose();
        _reader = null;

        _nativeMemoryStream?.Dispose();
        _nativeMemoryStream = null;

        DestroyStream();

        GC.SuppressFinalize(this);
    }
    
    private async Task<RecordBatch?> NextAsync()
    {
        ObjectDisposedException.ThrowIf(_handle == IntPtr.Zero, nameof(DataFrameStream));

        var (id, tcs) = AsyncOperations.Instance.Create<BytesData?>();
        var result = NativeMethods.DataFrameStreamNext(_handle, CallbackForNextResultHandler, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start getting next batch from stream");
        }

        var data = await tcs.Task.ConfigureAwait(false);
        if (data is null)
            return null;
        
        if (_nativeMemoryStream is null)
        {
            _nativeMemoryStream = new NativeMemoryStream();
            _nativeMemoryStream.SetNativeMemory(new NativeMemoryManager(data.Value.DataPtr, data.Value.Length));
            _reader = new ArrowStreamReader(_nativeMemoryStream);
        }
        else
            _nativeMemoryStream.SetNativeMemory(new NativeMemoryManager(data.Value.DataPtr, data.Value.Length));
        
        return await _reader!.ReadNextRecordBatchAsync().ConfigureAwait(false);
    }

    private static void CallbackForNextResult(IntPtr result, IntPtr error, ulong handle)
    {
        if (result == IntPtr.Zero && error == IntPtr.Zero)
        {
            // Null result - end of stream
            AsyncOperations.Instance.CompleteWithResult<BytesData?>(handle, null);
        }
        else if (error == IntPtr.Zero)
        {
            try
            {
                var data = BytesData.FromIntPtr(result);
                AsyncOperations.Instance.CompleteWithResult<BytesData?>(handle, data);
            }
            catch (Exception ex)
            {
                AsyncOperations.Instance.CompleteWithError<BytesData?>(handle, ex);
            }
        }
        else
        {
            AsyncOperations.Instance.CompleteWithError<BytesData?>(handle, ErrorInfoData.FromIntPtr(error).ToException());
        }
    }
    private static readonly NativeMethods.Callback CallbackForNextResultDelegate = CallbackForNextResult;
    private static readonly IntPtr CallbackForNextResultHandler = Marshal.GetFunctionPointerForDelegate(CallbackForNextResultDelegate);

    private void DestroyStream()
    {
        var handle = _handle;
        if (handle == IntPtr.Zero)
            return;

        _handle = IntPtr.Zero;

        NativeMethods.DataFrameStreamDestroy(handle);
    }
}
