using Apache.Arrow;
using DataFusionSharp.Interop;

namespace DataFusionSharp;

/// <summary>
/// An async stream of Arrow arrays as batches from a DataFrame query execution.
/// Uses zero-copy Arrow import, so the data is not copied into .NET-owned memory -
///   reference the memory allocated by native DataFusion runtime.
/// </summary>
/// <remarks>
/// It is important to dispose of the <see cref="DataFrameStream"/> when it is no longer needed to free the native resources.
/// Do not use the Arrow data after disposing, as it references memory owned by DataFusion that will be freed upon disposal.
/// To access the data after disposal, a cloning is necessary.
/// </remarks>
/// <example>
/// <code lang="csharp">
/// using var stream = dataFrame.ExecuteStream();
/// await foreach (var batch in stream)
///     ...
/// </code>
/// </example>
#pragma warning disable CA1711 // Identifiers should not have incorrect suffix
public sealed partial class DataFrameStream : IAsyncEnumerable<RecordBatch>, IDisposable
#pragma warning restore CA1711
{
    private readonly DataFrameStreamSafeHandle _handle;
    private readonly List<RecordBatch> _batches = [];

    /// <summary>
    /// Gets the <see cref="DataFusionSharp.DataFrame"/> that created this stream.
    /// </summary>
    public DataFrame DataFrame { get; }

    /// <summary>
    /// Gets the <see cref="Apache.Arrow.Schema" /> of the record batches produced by this stream.
    /// </summary>
    public Schema Schema { get; }
    
    internal DataFrameStream(DataFrame dataFrame, Schema schema, DataFrameStreamSafeHandle handle)
    {
        DataFrame = dataFrame;
        Schema = schema;
        _handle = handle;
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

        _handle.Dispose();
    }
    
    private async Task<RecordBatch?> NextAsync()
    {
        var (id, tcs) = AsyncOperations.Instance.Create<RecordBatch?, Schema>(Schema);
        
        var result = NativeMethods.DataFrameStreamNext(_handle, CallbackForNextResultHandle, id);
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

    [DataFusionSharpNativeCallback]
    private static unsafe void CallbackForNextResult(IntPtr result, IntPtr error, ulong userData)
    {
        if (error != IntPtr.Zero)
        {
            var ex = ErrorInfoData.FromIntPtr(error).ToException();
            AsyncOperations.Instance.CompleteWithError<RecordBatch?>(userData, ex);
            return;
        }
        
        if (result == IntPtr.Zero)
        {
            // Null result - end of stream
            AsyncOperations.Instance.CompleteWithResult<RecordBatch?>(userData, null);
            return;
        }

        var data = (Apache.Arrow.C.CArrowArray*)result.ToPointer();
        RecordBatch batch;
        try
        {
            var schema = AsyncOperations.Instance.GetUserData<Schema>(userData);
            if (schema is null)
                throw new InvalidOperationException("Failed to retrieve schema for next batch retrieval operation");

            batch = Apache.Arrow.C.CArrowArrayImporter.ImportRecordBatch(data, schema);
        }
        catch (Exception ex)
        {
            try
            {
                Apache.Arrow.C.CArrowArray.CallReleaseFunc(data);
            }
            catch
            {
                // Ignore exceptions from release function - we are already handling another exception and there's not much we can do about it.
            }

            AsyncOperations.Instance.CompleteWithError<RecordBatch?>(userData, ex);
            return;
        }
        
        AsyncOperations.Instance.CompleteWithResult<RecordBatch?>(userData, batch);
    }
}
