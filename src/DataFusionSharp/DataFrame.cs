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
    private readonly DataFrameSafeHandle _handle;

    /// <summary>
    /// Gets the session context that created this DataFrame.
    /// </summary>
    public SessionContext Context { get; }
    
    internal DataFrame(SessionContext sessionContext, DataFrameSafeHandle handle)
    {
        Context = sessionContext;
        _handle = handle;
    }
    
    /// <summary>
    /// Returns the number of rows in this DataFrame.
    /// </summary>
    /// <returns>A task containing the row count.</returns>
    /// <exception cref="DataFusionException">Thrown when the operation fails.</exception>
    public Task<ulong> CountAsync()
    {
        var (id, tcs) = AsyncOperations.Instance.Create<ulong>();
        var result = NativeMethods.DataFrameCount(_handle.GetHandle(), AsyncOperationGenericCallbacks.UInt64ResultHandler, id);
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
        var result = NativeMethods.DataFrameShow(_handle.GetHandle(), limit ?? 0, AsyncOperationGenericCallbacks.VoidResultHandler, id);
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
        var result = NativeMethods.DataFrameToString(_handle.GetHandle(), AsyncOperationGenericCallbacks.StringResultHandler, id);
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
        var result = NativeMethods.DataFrameSchema(_handle.GetHandle(), CallbackForSchemaResultHandle, id);
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
    /// <returns>A task containing the <see cref="DataFrameCollectedResult"/> with all record batches and schema.</returns>
    /// <exception cref="DataFusionException">Thrown when the operation fails.</exception>
    public Task<DataFrameCollectedResult> CollectAsync()
    {
        var (id, tcs) = AsyncOperations.Instance.Create<DataFrameCollectedResult>();
        var result = NativeMethods.DataFrameCollect(_handle.GetHandle(), CallbackForCollectResultHandle, id);
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
        var (id, tcs) = AsyncOperations.Instance.Create<(Schema Schema, IntPtr StreamHandle)>();
        var result = NativeMethods.DataFrameExecuteStream(_handle.GetHandle(), CallbackForExecutedStreamHandle, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start executing stream on DataFrame");
        }

        var (schema, streamHandle) = await tcs.Task.ConfigureAwait(false);
        return new DataFrameStream(this, schema, new DataFrameStreamSafeHandle(streamHandle));
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
        var result = NativeMethods.DataFrameWriteCsv(_handle.GetHandle(), path, optionsData.ToBytesData(), AsyncOperationGenericCallbacks.VoidResultHandler, id);
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
        var result = NativeMethods.DataFrameWriteJson(_handle.GetHandle(), path, AsyncOperationGenericCallbacks.VoidResultHandler, id);
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
        var result = NativeMethods.DataFrameWriteParquet(_handle.GetHandle(), path, AsyncOperationGenericCallbacks.VoidResultHandler, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start writing DataFrame to Parquet");
        }

        return tcs.Task;
    }
    
    /// <inheritdoc />
    public void Dispose()
    {
        _handle.Dispose();
    }
    
    private static unsafe void CallbackForGetSchema(IntPtr result, IntPtr error, ulong handle)
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
    private static readonly NativeMethods.Callback CallbackForSchemaResultDelegate = CallbackForGetSchema;
    private static readonly IntPtr CallbackForSchemaResultHandle = Marshal.GetFunctionPointerForDelegate(CallbackForSchemaResultDelegate);
    
    private static unsafe void CallbackForCollect(IntPtr result, IntPtr error, ulong handle)
    {
        if (error == IntPtr.Zero)
        {
            var data = (NativeDataFrameCollectedData*) result.ToPointer();
            try
            {
                var (schema, batches) = ImportCollectedData(data);
                
#pragma warning disable CA2000
                AsyncOperations.Instance.CompleteWithResult(handle, new DataFrameCollectedResult(batches.AsReadOnly(), schema));
#pragma warning restore CA2000
            }
            catch (Exception ex)
            {
                AsyncOperations.Instance.CompleteWithError<DataFrameCollectedResult>(handle, ex);
            }
        }
        else
            AsyncOperations.Instance.CompleteWithError<DataFrameCollectedResult>(handle, ErrorInfoData.FromIntPtr(error).ToException());
    }
    private static readonly NativeMethods.Callback CallbackForCollectResultDelegate = CallbackForCollect;
    private static readonly IntPtr CallbackForCollectResultHandle = Marshal.GetFunctionPointerForDelegate(CallbackForCollectResultDelegate);
    
    private static unsafe (Schema Schema, List<RecordBatch> Batches) ImportCollectedData(NativeDataFrameCollectedData* data)
    {
        var batches = new List<RecordBatch>();
        try
        {
            var schema = Apache.Arrow.C.CArrowSchemaImporter.ImportSchema(data->Schema);
            batches = new List<RecordBatch>(data->NumBatches);
            for (var i = 0; i < data->NumBatches; i++)
            {
                var batch = Apache.Arrow.C.CArrowArrayImporter.ImportRecordBatch(data->Batches + i, schema);
                batches.Add(batch);
            }

            return (schema, batches);
        }
        catch
        {
            try
            {
                for (var i = 0; i < data->NumBatches; i++)
                    Apache.Arrow.C.CArrowArray.CallReleaseFunc(data->Batches + i);
                
                foreach (var batch in batches)
                    batch.Dispose();
            }
            catch
            {
                // Ignore exceptions from release functions - we are already handling another exception and there's not much we can do about it.
            }

            throw;
        }
    }
    
    private static unsafe void CallbackForExecutedStream(IntPtr result, IntPtr error, ulong handle)
    {
        if (error == IntPtr.Zero)
        {
            try
            {
                var data = (NativeDataFrameExecutedStreamData*) result.ToPointer();
                var schema = Apache.Arrow.C.CArrowSchemaImporter.ImportSchema(data->Schema);
                
                AsyncOperations.Instance.CompleteWithResult(handle, ValueTuple.Create(schema, data->StreamHandle));
            }
            catch (Exception ex)
            {
                AsyncOperations.Instance.CompleteWithError<(Schema, IntPtr)>(handle, ex);
            }
        }
        else
            AsyncOperations.Instance.CompleteWithError<(Schema, IntPtr)>(handle, ErrorInfoData.FromIntPtr(error).ToException());
    }
    private static readonly NativeMethods.Callback CallbackForExecutedStreamDelegate = CallbackForExecutedStream;
    private static readonly IntPtr CallbackForExecutedStreamHandle = Marshal.GetFunctionPointerForDelegate(CallbackForExecutedStreamDelegate);
}

/// <summary>
/// Contains the collected Arrow arrays as batches and schema from a DataFrame.
/// Uses zero-copy Arrow import, so the data is not copied into .NET-owned memory -
///   reference the memory allocated by native DataFusion runtime.
/// </summary>
/// <remarks>
/// It is important to dispose of the <see cref="DataFrameCollectedResult"/> when it is no longer needed to free the native resources.
/// Do not use the Arrow data after disposing, as it references memory owned by DataFusion that will be freed upon disposal.
/// To access the data after disposal, a cloning is necessary.
/// </remarks>
/// <example>
/// <code language="csharp">
/// using var result = await dataFrame.CollectAsync();
/// var batches = result.Batches; // Access the collected record batches
/// var schema = result.Schema; // Access the schema of the collected batches
/// </code>
/// </example>
public sealed class DataFrameCollectedResult : IDisposable
{
    /// <summary>
    /// The collected record batches.
    /// </summary>
    public IReadOnlyList<RecordBatch> Batches { get; }
    
    /// <summary>
    /// The schema of the collected record batches.
    /// </summary>
    public Schema Schema { get; }
    
    internal DataFrameCollectedResult(IReadOnlyList<RecordBatch> batches, Schema schema)
    {
        Batches = batches;
        Schema = schema;
    }
    
    /// <inheritdoc />
    public void Dispose()
    {
        foreach(var batch in Batches)
            batch.Dispose();
    }
}
