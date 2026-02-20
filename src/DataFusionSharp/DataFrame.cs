using System.Runtime.InteropServices;
using Apache.Arrow;
using DataFusionSharp.Formats.Csv;
using DataFusionSharp.Interop;

namespace DataFusionSharp;

/// <summary>
/// Represents the result of a DataFusion query. Provides methods to collect, stream, or write results.
/// </summary>
/// <remarks>
/// A DataFrame is a lazy representation of a query plan. The query is not executed until a terminal operation
/// is called (e.g., <see cref="CollectAsync"/>, <see cref="ExecuteStreamAsync"/>, or one of the Write methods).
/// This class is not thread-safe. Do not call methods on the same instance concurrently from multiple threads.
/// </remarks>
public sealed class DataFrame : IDisposable
{
    private IntPtr _handle;

    /// <summary>
    /// Gets the session context that created this DataFrame.
    /// </summary>
    public SessionContext Context { get; }
    
    internal DataFrame(SessionContext sessionContext, IntPtr handle)
    {
        Context = sessionContext;
        _handle = handle;
    }
    
    /// <summary>
    /// Releases unmanaged resources if <see cref="Dispose"/> was not called.
    /// </summary>
    ~DataFrame()
    {
        DestroyDataFrame();
    }
    
    /// <summary>
    /// Returns the number of rows in this DataFrame.
    /// </summary>
    /// <returns>A task containing the row count.</returns>
    /// <exception cref="DataFusionException">Thrown when the operation fails.</exception>
    public Task<ulong> CountAsync()
    {
        var (id, tcs) = AsyncOperations.Instance.Create<ulong>();
        var result = NativeMethods.DataFrameCount(_handle, AsyncOperationGenericCallbacks.UInt64ResultHandler, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start counting rows in DataFrame");
        }
        return tcs.Task;
    }

    /// <summary>
    /// Prints the DataFrame contents to stdout.
    /// </summary>
    /// <param name="limit">Maximum number of rows to display. If null, displays all rows.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="DataFusionException">Thrown when the operation fails.</exception>
    public Task ShowAsync(ulong? limit = null)
    {
        if (limit.HasValue)
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit.Value);
        
        var (id, tcs) = AsyncOperations.Instance.Create();
        var result = NativeMethods.DataFrameShow(_handle, limit ?? 0, AsyncOperationGenericCallbacks.VoidResultHandler, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start showing DataFrame");
        }
        return tcs.Task;
    }
    
    /// <summary>
    /// Returns a string representation of the DataFrame contents.
    /// </summary>
    /// <returns>A task containing the string representation.</returns>
    /// <exception cref="DataFusionException">Thrown when the operation fails.</exception>
    public Task<string> ToStringAsync()
    {
        var (id, tcs) = AsyncOperations.Instance.Create<string>();
        var result = NativeMethods.DataFrameToString(_handle, AsyncOperationGenericCallbacks.StringResultHandler, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start converting DataFrame to string");
        }
        return tcs.Task;
    }
    
    /// <summary>
    /// Returns the Arrow schema of this DataFrame.
    /// </summary>
    /// <returns>A task containing the <see cref="Schema"/>.</returns>
    /// <exception cref="DataFusionException">Thrown when the operation fails.</exception>
    public Task<Schema> GetSchemaAsync()
    {
        var (id, tcs) = AsyncOperations.Instance.Create<Schema>();
        var result = NativeMethods.DataFrameSchema(_handle, CallbackForSchemaResultHandler, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start getting DataFrame schema");
        }

        return tcs.Task;
    }
    
    /// <summary>
    /// Collects all data from this DataFrame into memory.
    /// </summary>
    /// <returns>A task containing the <see cref="DataFrameCollectedData"/> with all record batches and schema.</returns>
    /// <exception cref="DataFusionException">Thrown when the operation fails.</exception>
    public Task<DataFrameCollectedData> CollectAsync()
    {
        var (id, tcs) = AsyncOperations.Instance.Create<DataFrameCollectedData>();
        var result = NativeMethods.DataFrameCollect(_handle, CallbackForCollectResultHandler, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start collecting DataFrame");
        }

        return tcs.Task;
    }
    
    /// <summary>
    /// Executes the query and returns a stream of record batches.
    /// </summary>
    /// <returns>A task containing a <see cref="DataFrameStream"/> for async enumeration.</returns>
    /// <exception cref="DataFusionException">Thrown when the operation fails.</exception>
    public async Task<DataFrameStream> ExecuteStreamAsync()
    {
        var (id, tcs) = AsyncOperations.Instance.Create<IntPtr>();
        var result = NativeMethods.DataFrameExecuteStream(_handle, AsyncOperationGenericCallbacks.IntPtrResultHandler, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start executing stream on DataFrame");
        }

        var streamHandle = await tcs.Task.ConfigureAwait(false);
        return new DataFrameStream(this, streamHandle);
    }

    /// <summary>
    /// Writes the DataFrame contents to a CSV file.
    /// </summary>
    /// <param name="path">The output file path.</param>
    /// <param name="options">Optional CSV writing options.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="DataFusionException">Thrown when the operation fails.</exception>
    public Task WriteCsvAsync(string path, CsvWriteOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);

        using var optionsData = PinnedProtobufData.FromMessage(options?.ToProto());

        var (id, tcs) = AsyncOperations.Instance.Create();
        var result = NativeMethods.DataFrameWriteCsv(_handle, path, optionsData.ToBytesData(), AsyncOperationGenericCallbacks.VoidResultHandler, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start writing DataFrame to CSV");
        }

        return tcs.Task;
    }

    /// <summary>
    /// Writes the DataFrame contents to a JSON file.
    /// </summary>
    /// <param name="path">The output file path.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="DataFusionException">Thrown when the operation fails.</exception>
    public Task WriteJsonAsync(string path)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        
        var (id, tcs) = AsyncOperations.Instance.Create();
        var result = NativeMethods.DataFrameWriteJson(_handle, path, AsyncOperationGenericCallbacks.VoidResultHandler, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start writing DataFrame to JSON");
        }

        return tcs.Task;
    }

    /// <summary>
    /// Writes the DataFrame contents to a Parquet file.
    /// </summary>
    /// <param name="path">The output file path.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="DataFusionException">Thrown when the operation fails.</exception>
    public Task WriteParquetAsync(string path)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        
        var (id, tcs) = AsyncOperations.Instance.Create();
        var result = NativeMethods.DataFrameWriteParquet(_handle, path, AsyncOperationGenericCallbacks.VoidResultHandler, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start writing DataFrame to Parquet");
        }

        return tcs.Task;
    }
    
    /// <summary>
    /// Releases all resources used by this DataFrame.
    /// </summary>
    public void Dispose()
    {
        DestroyDataFrame();
        GC.SuppressFinalize(this);
    }
    
    private static unsafe void CallbackForSchemaResult(IntPtr result, IntPtr error, ulong handle)
    {
        if (error == IntPtr.Zero)
        {
            try
            {
                var schema = Apache.Arrow.C.CArrowSchemaImporter.ImportSchema((Apache.Arrow.C.CArrowSchema*) result.ToPointer());
                AsyncOperations.Instance.CompleteWithResult(handle, schema);
            }
            catch (Exception ex)
            {
                AsyncOperations.Instance.CompleteWithError<Schema>(handle, ex);
            }
        }
        else
            AsyncOperations.Instance.CompleteWithError<Schema>(handle, ErrorInfoData.FromIntPtr(error).ToException());
    }
    private static readonly NativeMethods.Callback CallbackForSchemaResultDelegate = CallbackForSchemaResult;
    private static readonly IntPtr CallbackForSchemaResultHandler = Marshal.GetFunctionPointerForDelegate(CallbackForSchemaResultDelegate);
    
    private static unsafe void CallbackForCollectResult(IntPtr result, IntPtr error, ulong handle)
    {
        if (error == IntPtr.Zero)
        {
            try
            {
                var data = (CollectedRecordBatches*) result.ToPointer();
                var schema = Apache.Arrow.C.CArrowSchemaImporter.ImportSchema(data->Schema);
                var batches = new List<RecordBatch>(data->NumBatches);
                for (var i = 0; i < data->NumBatches; i++)
                {
                    var batch = Apache.Arrow.C.CArrowArrayImporter.ImportRecordBatch(data->Batches + i, schema);
                    batches.Add(batch);
                }
                
#pragma warning disable CA2000
                AsyncOperations.Instance.CompleteWithResult(handle, new DataFrameCollectedData(batches.AsReadOnly(), schema));
#pragma warning restore CA2000
            }
            catch (Exception ex)
            {
                AsyncOperations.Instance.CompleteWithError<DataFrameCollectedData>(handle, ex);
            }
        }
        else
            AsyncOperations.Instance.CompleteWithError<DataFrameCollectedData>(handle, ErrorInfoData.FromIntPtr(error).ToException());
    }
    private static readonly NativeMethods.Callback CallbackForCollectResultDelegate = CallbackForCollectResult;
    private static readonly IntPtr CallbackForCollectResultHandler = Marshal.GetFunctionPointerForDelegate(CallbackForCollectResultDelegate);
    
    private void DestroyDataFrame()
    {
        var handle = _handle;
        if (handle == IntPtr.Zero)
            return;
        
        _handle = IntPtr.Zero;
        
        NativeMethods.DataFrameDestroy(handle);
    }
}

/// <summary>
/// Contains the collected record batches and schema from a DataFrame.
/// </summary>
/// <param name="Batches">The list of collected record batches.</param>
/// <param name="Schema">The Arrow schema of the data.</param>
public sealed record DataFrameCollectedData(IReadOnlyList<RecordBatch> Batches, Schema Schema) : IDisposable
{
    /// <inheritdoc />
    public void Dispose()
    {
        foreach(var batch in Batches)
            batch.Dispose();
    }
}
