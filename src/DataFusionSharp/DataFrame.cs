using System.Runtime.ExceptionServices;
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
public sealed partial class DataFrame : IDisposable, ICloneable
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

        using var paramValuesData = PinnedProtobufData.FromMessage(parameters.ToProto());
        var (id, tcs) = AsyncOperations.Instance.Create();
        var result = NativeMethods.DataFrameWithParameters(_handle, paramValuesData.ToBytesData(), GenericCallbacks.CallbackForVoidHandle, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to parameterize DataFrame with SQL parameters");
        }

        switch (tcs.Task.Status)
        {
            case TaskStatus.RanToCompletion:
                return this;
            case TaskStatus.Faulted:
                if (tcs.Task.Exception != null)
                    ExceptionDispatchInfo.Throw(tcs.Task.Exception.InnerException ?? tcs.Task.Exception);
                throw new DataFusionException(DataFusionErrorCode.Panic, "DataFrameWithParameters task faulted without exception");
            default:
                throw new DataFusionException(DataFusionErrorCode.Panic, "Unexpected asynchronous completion of DataFrameWithParameters native method");
        }
    }

    /// <summary>
    /// Returns the number of rows in this DataFrame.
    /// </summary>
    /// <returns>A task containing the row count.</returns>
    /// <exception cref="DataFusionException">Thrown when the operation fails.</exception>
    public Task<ulong> CountAsync()
    {
        var (id, tcs) = AsyncOperations.Instance.Create<ulong>();
        var result = NativeMethods.DataFrameCount(_handle, CallbackForCountAsyncHandle, id);
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
        var result = NativeMethods.DataFrameShow(_handle, limit ?? 0, GenericCallbacks.CallbackForVoidHandle, id);
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
        var result = NativeMethods.DataFrameToString(_handle, GenericCallbacks.CallbackForStringHandle, id);
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
        var result = NativeMethods.DataFrameSchema(_handle, CallbackForGetSchemaHandle, id);
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
        var result = NativeMethods.DataFrameCollect(_handle, CallbackForCollectHandle, id);
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
        var (id, tcs) = AsyncOperations.Instance.Create<(Schema Schema, DataFrameStreamSafeHandle StreamHandle)>();
        var result = NativeMethods.DataFrameExecuteStream(_handle, CallbackForExecutedStreamHandle, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start executing stream on DataFrame");
        }

        var (schema, streamHandle) = await tcs.Task.ConfigureAwait(false);
        return new DataFrameStream(this, schema, streamHandle);
    }

    /// <summary>
    /// Writes the DataFrame contents to a CSV file.
    /// </summary>
    /// <param name="path">The output file path.</param>
    /// <param name="dataFrameWriteOptions">Optional DataFrame writing options.</param>
    /// <param name="csvWriteOptions">Optional CSV writing options.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="DataFusionException">Thrown when the operation fails.</exception>
    public Task WriteCsvAsync(string path, DataFrameWriteOptions? dataFrameWriteOptions = null, CsvWriteOptions? csvWriteOptions = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);

        using var dataFrameOptionsData = PinnedProtobufData.FromMessage(dataFrameWriteOptions?.ToProto());
        using var csvOptionsData = PinnedProtobufData.FromMessage(csvWriteOptions?.ToProto());

        var (id, tcs) = AsyncOperations.Instance.Create();
        var result = NativeMethods.DataFrameWriteCsv(_handle, path,
            dataFrameOptionsData.ToBytesData(), csvOptionsData.ToBytesData(),
            GenericCallbacks.CallbackForVoidHandle, id);
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
    /// <param name="dataFrameWriteOptions">Optional DataFrame writing options.</param>
    /// <param name="jsonWriteOptions">Optional JSON writing jsonWriteOptions.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="DataFusionException">Thrown when the operation fails.</exception>
    public Task WriteJsonAsync(string path, DataFrameWriteOptions? dataFrameWriteOptions = null, JsonWriteOptions? jsonWriteOptions = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);

        using var dataFrameOptionsData = PinnedProtobufData.FromMessage(dataFrameWriteOptions?.ToProto());
        using var optionsData = PinnedProtobufData.FromMessage(jsonWriteOptions?.ToProto());

        var (id, tcs) = AsyncOperations.Instance.Create();
        var result = NativeMethods.DataFrameWriteJson(_handle, path,
            dataFrameOptionsData.ToBytesData(), optionsData.ToBytesData(), 
            GenericCallbacks.CallbackForVoidHandle, id);
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
    /// <param name="dataFrameWriteOptions">Optional DataFrame writing options.</param>
    /// <param name="parquetWriteOptions">Optional Parquet writing options.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="DataFusionException">Thrown when the operation fails.</exception>
    public Task WriteParquetAsync(string path, DataFrameWriteOptions? dataFrameWriteOptions = null, ParquetWriteOptions? parquetWriteOptions = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);

        using var dataFrameOptionsData = PinnedProtobufData.FromMessage(dataFrameWriteOptions?.ToProto());
        using var parquetOptionsData = PinnedProtobufData.FromMessage(parquetWriteOptions?.ToProto());

        var (id, tcs) = AsyncOperations.Instance.Create();
        var result = NativeMethods.DataFrameWriteParquet(_handle, path,
            dataFrameOptionsData.ToBytesData(), parquetOptionsData.ToBytesData(),
            GenericCallbacks.CallbackForVoidHandle, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start writing DataFrame to Parquet");
        }

        return tcs.Task;
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
    
    [DataFusionSharpNativeCallback]
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
    
    [DataFusionSharpNativeCallback]
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
    
    [DataFusionSharpNativeCallback]
    private static unsafe void CallbackForGetSchema(IntPtr result, IntPtr error, ulong handle)
    {
        if (error != IntPtr.Zero)
        {
            var ex = ErrorInfoData.FromIntPtr(error).ToException();
            AsyncOperations.Instance.CompleteWithError<Schema>(handle, ex);
            return;
        }

        Schema schema;
        try
        {
            schema = Apache.Arrow.C.CArrowSchemaImporter.ImportSchema((Apache.Arrow.C.CArrowSchema*)result.ToPointer());
        }
        catch (Exception ex)
        {
            AsyncOperations.Instance.CompleteWithError<Schema>(handle, ex);
            return;
        }
        
        AsyncOperations.Instance.CompleteWithResult(handle, schema);
    }
    
    [DataFusionSharpNativeCallback]
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
    
    [DataFusionSharpNativeCallback]
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
/// Represents a named SQL parameter with its value and metadata. This is used for parameterizing SQL queries in DataFusion.
/// </summary>
public readonly record struct NamedScalarValueAndMetadata
{
    /// <summary>
    /// Gets the name of the parameter.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the value of the parameter.
    /// </summary>
    public ScalarValueAndMetadata Value { get; }
    
    /// <summary>
    /// Initializes a new instance of the <see cref="NamedScalarValueAndMetadata"/> record struct with the specified name and value.
    /// </summary>
    /// <param name="name">The name of the parameter. Must not be null, empty, or whitespace, and must not start with the '$' symbol.</param>
    /// <param name="value">The value of the parameter, including its metadata.</param>
    /// <exception cref="ArgumentException">Thrown when invalid parameters.</exception>
    public NamedScalarValueAndMetadata(string name, ScalarValueAndMetadata value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        if (name.StartsWith('$'))
            throw new ArgumentException("Parameter name should not start with '$' symbol", nameof(name));

        Name = name;
        Value = value;
    }
    
    /// <summary>
    /// Initializes a new instance of the <see cref="NamedScalarValueAndMetadata"/> record struct with the specified name and scalar value.
    /// </summary>
    /// <param name="name">The name of the parameter. Must not be null, empty, or whitespace, and must not start with the '$' symbol.</param>
    /// <param name="value">The scalar value of the parameter.</param>
    public NamedScalarValueAndMetadata(string name, ScalarValue value)
        : this(name, new ScalarValueAndMetadata(value))
    {
    }
    
    /// <summary>
    /// Implicitly converts a tuple of (string Name, ScalarValueAndMetadata Value) to a <see cref="NamedScalarValueAndMetadata"/> record struct.
    /// </summary>
    /// <param name="tuple">The tuple containing the name and value to convert.</param>
    /// <returns>>A new instance of <see cref="NamedScalarValueAndMetadata"/>.</returns>
    public static implicit operator NamedScalarValueAndMetadata((string Name, ScalarValueAndMetadata Value) tuple) => new(tuple.Name, tuple.Value);
    
    /// <summary>
    /// Converts a tuple of (string Name, ScalarValueAndMetadata Value) to a <see cref="NamedScalarValueAndMetadata"/> record struct.
    /// </summary>
    /// <param name="tuple">The tuple containing the name and value to convert.</param>
    /// <returns>>A new instance of <see cref="NamedScalarValueAndMetadata"/>.</returns>
    public static NamedScalarValueAndMetadata ToNamedScalarValueAndMetadata((string Name, ScalarValueAndMetadata Value) tuple) => new(tuple.Name, tuple.Value);
    
    /// <summary>
    /// Implicitly converts a tuple of (string Name, ScalarValue Value) to a <see cref="NamedScalarValueAndMetadata"/> record struct.
    /// </summary>
    /// <param name="tuple">The tuple containing the name and value to convert.</param>
    /// <returns>>A new instance of <see cref="NamedScalarValueAndMetadata"/>.</returns>
    public static implicit operator NamedScalarValueAndMetadata((string Name, ScalarValue Value) tuple) => new(tuple.Name, tuple.Value);
    
    /// <summary>
    /// Converts a tuple of (string Name, ScalarValue Value) to a <see cref="NamedScalarValueAndMetadata"/> record struct.
    /// </summary>
    /// <param name="tuple">The tuple containing the name and value to convert.</param>
    /// <returns>>A new instance of <see cref="NamedScalarValueAndMetadata"/>.</returns>
    public static NamedScalarValueAndMetadata ToNamedScalarValueAndMetadata((string Name, ScalarValue Value) tuple) => new(tuple.Name, tuple.Value);
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
