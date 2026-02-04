using Apache.Arrow;
using Apache.Arrow.Ipc;
using DataFusionSharp.Interop;

namespace DataFusionSharp;

public sealed class DataFrameStream : IAsyncEnumerable<RecordBatch>, IDisposable
{
    private IntPtr _handle;
    private NativeMemoryStream? _nativeMemoryStream;
    private ArrowStreamReader? _reader;

    public DataFrame DataFrame { get; }

    internal DataFrameStream(DataFrame dataFrame, IntPtr handle)
    {
        DataFrame = dataFrame;
        _handle = handle;
    }

    ~DataFrameStream()
    {
        DestroyStream();
    }

    public async IAsyncEnumerator<RecordBatch> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        while (await NextAsync() is { } batch)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return batch;
        }
    }

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
        if (_handle == IntPtr.Zero)
            throw new ObjectDisposedException(nameof(DataFrameStream));

        var (id, tcs) = AsyncOperations.Instance.Create<BytesData?>();
        var result = NativeMethods.DataFrameStreamNext(_handle, CallbackForNextResult, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start getting next batch from stream");
        }

        var data = await tcs.Task;
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
        
        return await _reader!.ReadNextRecordBatchAsync();
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

    private void DestroyStream()
    {
        var handle = _handle;
        if (handle == IntPtr.Zero)
            return;

        _handle = IntPtr.Zero;

        NativeMethods.DataFrameStreamDestroy(handle);
    }
}
