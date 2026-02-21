using System.Runtime.InteropServices;
using Apache.Arrow;
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
    private IntPtr _streamHandle;
    private readonly List<RecordBatch> _batches = [];

    /// <summary>
    /// Gets the <see cref="DataFusionSharp.DataFrame"/> that created this stream.
    /// </summary>
    public DataFrame DataFrame { get; }

    /// <summary>
    /// Gets the <see cref="Apache.Arrow.Schema" /> of the record batches produced by this stream.
    /// </summary>
    public Schema Schema { get; }
    
    internal DataFrameStream(DataFrame dataFrame, Schema schema, IntPtr streamHandle)
    {
        DataFrame = dataFrame;
        Schema = schema;
        _streamHandle = streamHandle;
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
        _batches.ForEach(batch => batch.Dispose());
        _batches.Clear();

        DestroyStream();

        GC.SuppressFinalize(this);
    }
    
    private async Task<RecordBatch?> NextAsync()
    {
        ObjectDisposedException.ThrowIf(_streamHandle == IntPtr.Zero, nameof(DataFrameStream));

        var (id, tcs) = AsyncOperations.Instance.Create<RecordBatch?, Schema>(Schema);
        
        var result = NativeMethods.DataFrameStreamNext(_streamHandle, CallbackForNextResultHandle, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start getting next batch from stream");
        }
        
        var batch = await tcs.Task.ConfigureAwait(false);

        if (batch is not null)
            _batches.Add(batch); // Keep track of batches to dispose them when the stream is disposed.
        
        return batch;
    }

    private static unsafe void CallbackForNextResult(IntPtr result, IntPtr error, ulong userData)
    {
        if (result == IntPtr.Zero && error == IntPtr.Zero)
        {
            // Null result - end of stream
            AsyncOperations.Instance.CompleteWithResult<RecordBatch?>(userData, null);
        }
        else if (error == IntPtr.Zero)
        {
            try
            {
                var data = (Apache.Arrow.C.CArrowArray*) result.ToPointer();
                
                var schema = AsyncOperations.Instance.GetUserData<Schema>(userData);
                if (schema is null)
                {
                    Apache.Arrow.C.CArrowArray.CallReleaseFunc(data);
                    throw new InvalidOperationException("Failed to retrieve schema for next batch retrieval operation");
                }
                
                var batch = Apache.Arrow.C.CArrowArrayImporter.ImportRecordBatch(data, schema);
                
                AsyncOperations.Instance.CompleteWithResult<RecordBatch?>(userData, batch);
            }
            catch (Exception ex)
            {
                AsyncOperations.Instance.CompleteWithError<RecordBatch?>(userData, ex);
            }
        }
        else
        {
            AsyncOperations.Instance.CompleteWithError<RecordBatch?>(userData, ErrorInfoData.FromIntPtr(error).ToException());
        }
    }
    private static readonly NativeMethods.Callback CallbackForNextResultDelegate = CallbackForNextResult;
    private static readonly IntPtr CallbackForNextResultHandle = Marshal.GetFunctionPointerForDelegate(CallbackForNextResultDelegate);

    private void DestroyStream()
    {
        var handle = _streamHandle;
        if (handle == IntPtr.Zero)
            return;

        _streamHandle = IntPtr.Zero;

        NativeMethods.DataFrameStreamDestroy(handle);
    }
}
