using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using DataFusionSharp.Formats;
using DataFusionSharp.Formats.Csv;
using DataFusionSharp.Formats.Json;
using DataFusionSharp.Formats.Parquet;
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
public sealed class DataFrame : IDisposable, ICloneable
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
    /// Parameterizes DataFrame with the provided SQL parameters. This is used to bind values to parameter placeholders in the original SQL query that created this DataFrame.
    /// </summary>
    /// <param name="parameters">A named parameters to bind to the query.</param>
    /// <returns>A DataFrame instance for chaining.</returns>
    /// <exception cref="DataFusionException">Thrown when query execution fails.</exception>
    /// <example>
    /// <code language="csharp">
    /// using var df = await session.SqlAsync("SELECT * FROM my_table WHERE id = $id");
    /// df.WithParameters([("id", 123)]);
    /// </code>
    /// </example>
    public DataFrame WithParameters(IEnumerable<NamedScalarValueAndMetadata> parameters)
    {
        ArgumentNullException.ThrowIfNull(parameters);

        using var paramValuesData = PinnedBytesData.FromMessage(parameters.ToProto());

        unsafe
        {
            var id = SyncOperations.Instance.Create();
            var result = NativeMethods.DataFrameWithParameters(_handle, paramValuesData.ToBytesData(), &GenericCallbacks.CallbackForVoidSync, id);
            SyncOperations.Instance.TakeResult(id, result, "Failed to start DataFrame with SQL parameters parameterization.");
        }

        return this;
    }

    /// <summary>
    /// Returns the number of rows in this DataFrame.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
    /// <returns>A task containing the row count.</returns>
    /// <exception cref="DataFusionException">Thrown when the operation fails.</exception>
    public Task<ulong> CountAsync(CancellationToken cancellationToken = default)
    {
        unsafe
        {
            var (id, tcs) = AsyncOperations.Instance.Create<ulong>(cancellationToken);
            var result = NativeMethods.DataFrameCount(_handle, &CallbackForCountAsync, id);
            AsyncOperations.Instance.EnsureNativeCall(id, result, "Failed to start counting rows in DataFrame.", cancellationToken);

            return tcs.Task;
        }
    }

    /// <summary>
    /// Prints the DataFrame contents to stdout.
    /// </summary>
    /// <param name="limit">Maximum number of rows to display. If null, displays all rows.</param>
    /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="DataFusionException">Thrown when the operation fails.</exception>
    public Task ShowAsync(ulong? limit = null, CancellationToken cancellationToken = default)
    {
        if (limit.HasValue)
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit.Value);

        unsafe
        {
            var (id, tcs) = AsyncOperations.Instance.Create(cancellationToken);
            var result = NativeMethods.DataFrameShow(_handle, limit ?? 0, &GenericCallbacks.CallbackForVoid, id);
            AsyncOperations.Instance.EnsureNativeCall(id, result, "Failed to start showing DataFrame.", cancellationToken);

            return tcs.Task;
        }
    }
    
    /// <summary>
    /// Returns a string representation of the DataFrame contents.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
    /// <returns>A task containing the string representation.</returns>
    /// <exception cref="DataFusionException">Thrown when the operation fails.</exception>
    public Task<string> ToStringAsync(CancellationToken cancellationToken = default)
    {
        unsafe
        {
            var (id, tcs) = AsyncOperations.Instance.Create<string>(cancellationToken);
            var result = NativeMethods.DataFrameToString(_handle, &GenericCallbacks.CallbackForString, id);
            AsyncOperations.Instance.EnsureNativeCall(id, result, "Failed to start converting DataFrame to string.", cancellationToken);

            return tcs.Task;
        }
    }
    
    /// <summary>
    /// Returns the Arrow schema of this DataFrame.
    /// </summary>
    /// <returns>A <see cref="Schema"/>.</returns>
    /// <exception cref="DataFusionException">Thrown when the operation fails.</exception>
    public Schema GetSchema()
    {
        unsafe
        {
            var id = SyncOperations.Instance.Create();
            var result = NativeMethods.DataFrameSchema(_handle, &CallbackForGetSchema, id);

            return SyncOperations.Instance.TakeResult<Schema>(id, result, "Failed to start getting DataFrame schema.");
        }
    }
    
    /// <summary>
    /// Collects all data from this DataFrame into memory.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
    /// <returns>A task containing the <see cref="DataFrameCollectedResult"/> with all record batches and schema.</returns>
    /// <exception cref="DataFusionException">Thrown when the operation fails.</exception>
    public Task<DataFrameCollectedResult> CollectAsync(CancellationToken cancellationToken = default)
    {
        unsafe
        {
            var (id, tcs) = AsyncOperations.Instance.Create<DataFrameCollectedResult>(cancellationToken);
            var result = NativeMethods.DataFrameCollect(_handle, &CallbackForCollect, id);
            AsyncOperations.Instance.EnsureNativeCall(id, result, "Failed to start collecting DataFrame.", cancellationToken);

            return tcs.Task;
        }
    }
    
    /// <summary>
    /// Executes the query and returns a stream of record batches.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
    /// <returns>A task containing a <see cref="DataFrameStream"/> for async enumeration.</returns>
    /// <exception cref="DataFusionException">Thrown when the operation fails.</exception>
    public async Task<DataFrameStream> ExecuteStreamAsync(CancellationToken cancellationToken = default)
    {
        Task<(Schema Schema, DataFrameStreamSafeHandle StreamHandle)> executeStreamTask;
        
        unsafe
        {
            var (id, tcs) = AsyncOperations.Instance.Create<(Schema Schema, DataFrameStreamSafeHandle StreamHandle)>(cancellationToken);
            var result = NativeMethods.DataFrameExecuteStream(_handle, &CallbackForExecutedStream, id);
            AsyncOperations.Instance.EnsureNativeCall(id, result, "Failed to start executing stream on DataFrame.", cancellationToken);
            executeStreamTask = tcs.Task;
        }
        
        var (schema, streamHandle) = await executeStreamTask.ConfigureAwait(false);

        return new DataFrameStream(this, schema, streamHandle);
    }

    /// <summary>
    /// Writes the DataFrame contents to a CSV file.
    /// </summary>
    /// <param name="path">The output file path.</param>
    /// <param name="dataFrameWriteOptions">Optional DataFrame writing options.</param>
    /// <param name="csvWriteOptions">Optional CSV writing options.</param>
    /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="DataFusionException">Thrown when the operation fails.</exception>
    public Task WriteCsvAsync(string path, DataFrameWriteOptions? dataFrameWriteOptions = null, CsvWriteOptions? csvWriteOptions = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);

        using var dataFrameOptionsData = PinnedBytesData.FromMessage(dataFrameWriteOptions?.ToProto());
        using var csvOptionsData = PinnedBytesData.FromMessage(csvWriteOptions?.ToProto());

        unsafe
        {
            var (id, tcs) = AsyncOperations.Instance.Create(cancellationToken);
            var result = NativeMethods.DataFrameWriteCsv(_handle, path, dataFrameOptionsData.ToBytesData(), csvOptionsData.ToBytesData(), &GenericCallbacks.CallbackForVoid, id);
            AsyncOperations.Instance.EnsureNativeCall(id, result, "Failed to start writing DataFrame to CSV.", cancellationToken);

            return tcs.Task;
        }
    }

    /// <summary>
    /// Writes the DataFrame contents to a JSON file.
    /// </summary>
    /// <param name="path">The output file path.</param>
    /// <param name="dataFrameWriteOptions">Optional DataFrame writing options.</param>
    /// <param name="jsonWriteOptions">Optional JSON writing jsonWriteOptions.</param>
    /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="DataFusionException">Thrown when the operation fails.</exception>
    public Task WriteJsonAsync(string path, DataFrameWriteOptions? dataFrameWriteOptions = null, JsonWriteOptions? jsonWriteOptions = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);

        using var dataFrameOptionsData = PinnedBytesData.FromMessage(dataFrameWriteOptions?.ToProto());
        using var optionsData = PinnedBytesData.FromMessage(jsonWriteOptions?.ToProto());

        unsafe
        {
            var (id, tcs) = AsyncOperations.Instance.Create(cancellationToken);
            var result = NativeMethods.DataFrameWriteJson(_handle, path, dataFrameOptionsData.ToBytesData(), optionsData.ToBytesData(), &GenericCallbacks.CallbackForVoid, id);
            AsyncOperations.Instance.EnsureNativeCall(id, result, "Failed to start writing DataFrame to JSON.", cancellationToken);

            return tcs.Task;
        }
    }

    /// <summary>
    /// Writes the DataFrame contents to a Parquet file.
    /// </summary>
    /// <param name="path">The output file path.</param>
    /// <param name="dataFrameWriteOptions">Optional DataFrame writing options.</param>
    /// <param name="parquetWriteOptions">Optional Parquet writing options.</param>
    /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="DataFusionException">Thrown when the operation fails.</exception>
    public Task WriteParquetAsync(string path, DataFrameWriteOptions? dataFrameWriteOptions = null, ParquetWriteOptions? parquetWriteOptions = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);

        using var dataFrameOptionsData = PinnedBytesData.FromMessage(dataFrameWriteOptions?.ToProto());
        using var parquetOptionsData = PinnedBytesData.FromMessage(parquetWriteOptions?.ToProto());

        unsafe
        {
            var (id, tcs) = AsyncOperations.Instance.Create(cancellationToken);
            var result = NativeMethods.DataFrameWriteParquet(_handle, path, dataFrameOptionsData.ToBytesData(), parquetOptionsData.ToBytesData(), &GenericCallbacks.CallbackForVoid, id);
            AsyncOperations.Instance.EnsureNativeCall(id, result, "Failed to start writing DataFrame to Parquet.", cancellationToken);

            return tcs.Task;
        }
    }
    
    /// <summary>
    /// Creates a deep clone of this DataFrame.
    /// The cloned DataFrame will have its own independent query execution and lifecycle, allowing it to be used concurrently with the original DataFrame without interference.
    /// </summary>
    /// <returns>A cloned <see cref="DataFrame"/>.</returns>
    public DataFrame Clone()
    {
        var clonedDataFrame = NativeMethods.DataFrameClone(_handle);
        var clonedDataFrameSafeHandle = new DataFrameSafeHandle(clonedDataFrame);
        return new DataFrame(Context, clonedDataFrameSafeHandle);
    }
    
    object ICloneable.Clone() => Clone();
    
    /// <inheritdoc />
    public void Dispose()
    {
        _handle.Dispose();
    }
    
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void CallbackForClone(IntPtr result, IntPtr error, ulong handle)
    {
        if (error != IntPtr.Zero)
        {
            var ex = ErrorInfoData.FromIntPtr(error).ToException();
            AsyncOperations.Instance.CompleteWithError<DataFrameSafeHandle>(handle, ex);
            return;
        }

        var dataFrameHandle = Marshal.ReadIntPtr(result);
#pragma warning disable CA2000
        var clonedDataFrameSafeHandle = new DataFrameSafeHandle(dataFrameHandle);
#pragma warning restore CA2000
        AsyncOperations.Instance.CompleteWithResult(handle, clonedDataFrameSafeHandle);
    }
    
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void CallbackForCountAsync(IntPtr result, IntPtr error, ulong handle)
    {
        if (error != IntPtr.Zero)
        {
            var ex = ErrorInfoData.FromIntPtr(error).ToException();
            AsyncOperations.Instance.CompleteWithError<ulong>(handle, ex);
            return;
        }

        AsyncOperations.Instance.CompleteWithResult(handle, (ulong)Marshal.ReadInt64(result));
    }
    
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe void CallbackForGetSchema(IntPtr result, IntPtr error, ulong handle)
    {
        if (error != IntPtr.Zero)
        {
            var ex = ErrorInfoData.FromIntPtr(error).ToException();
            SyncOperations.Instance.CompleteWithError<Schema>(handle, ex);
            return;
        }

        Schema schema;
        try
        {
            schema = Apache.Arrow.C.CArrowSchemaImporter.ImportSchema((Apache.Arrow.C.CArrowSchema*)result.ToPointer());
        }
        catch (Exception ex)
        {
            SyncOperations.Instance.CompleteWithError<Schema>(handle, ex);
            return;
        }
        
        SyncOperations.Instance.CompleteWithResult(handle, schema);
    }
    
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe void CallbackForCollect(IntPtr result, IntPtr error, ulong handle)
    {
        if (error != IntPtr.Zero)
        {
            var ex = ErrorInfoData.FromIntPtr(error).ToException();
            AsyncOperations.Instance.CompleteWithError<DataFrameCollectedResult>(handle, ex);
            return;
        }

        var data = (NativeDataFrameCollectedData*)result.ToPointer();
        Schema schema;
        List<RecordBatch> batches;
        try
        {
            (schema, batches) = ImportCollectedData(data);
        }
        catch (Exception ex)
        {
            AsyncOperations.Instance.CompleteWithError<DataFrameCollectedResult>(handle, ex);
            return;
        }
        
#pragma warning disable CA2000
        var collectedResult = new DataFrameCollectedResult(batches.AsReadOnly(), schema);
#pragma warning restore CA2000
        AsyncOperations.Instance.CompleteWithResult(handle, collectedResult);
    }
    
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
    
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe void CallbackForExecutedStream(IntPtr result, IntPtr error, ulong handle)
    {
        if (error != IntPtr.Zero)
        {
            var ex = ErrorInfoData.FromIntPtr(error).ToException();
            AsyncOperations.Instance.CompleteWithError<(Schema, DataFrameStreamSafeHandle)>(handle, ex);
            return;
        }

        var data = (NativeDataFrameExecutedStreamData*)result.ToPointer();
        Schema schema;
        try
        {
            schema = Apache.Arrow.C.CArrowSchemaImporter.ImportSchema(data->Schema);
        }
        catch (Exception ex)
        {
            AsyncOperations.Instance.CompleteWithError<(Schema, DataFrameStreamSafeHandle)>(handle, ex);
            return;
        }

#pragma warning disable CA2000
        var streamSafeHandle = new DataFrameStreamSafeHandle(data->StreamHandle);
#pragma warning restore CA2000
        AsyncOperations.Instance.CompleteWithResult(handle, ValueTuple.Create(schema, streamSafeHandle));
    }
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
