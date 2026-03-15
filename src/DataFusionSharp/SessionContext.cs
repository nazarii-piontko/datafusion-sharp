using System.Runtime.InteropServices;
using DataFusionSharp.Formats.Csv;
using DataFusionSharp.Formats.Json;
using DataFusionSharp.Formats.Parquet;
using DataFusionSharp.Interop;
using DataFusionSharp.ObjectStore;

namespace DataFusionSharp;

/// <summary>
/// Manages a DataFusion query session and provides methods for registering tables and executing SQL.
/// </summary>
/// <remarks>
/// A session context maintains its own catalog of registered tables and configuration state.
/// Multiple session contexts can be created from a single <see cref="DataFusionRuntime"/> for isolated query environments.
/// This class is not thread-safe. Do not call methods on the same instance concurrently from multiple threads.
/// </remarks>
public sealed partial class SessionContext : IDisposable
{
    private readonly SessionContextSafeHandle _handle;

    /// <summary>
    /// Gets the runtime that owns this session context.
    /// </summary>
    public DataFusionRuntime Runtime { get; }
    
    internal SessionContext(DataFusionRuntime runtime, SessionContextSafeHandle handle)
    {
        Runtime = runtime;
        _handle = handle;
    }

    /// <summary>
    /// Registers a CSV file as a table in this session.
    /// </summary>
    /// <param name="tableName">The name to use for the table.</param>
    /// <param name="filePath">The path to the CSV file.</param>
    /// <param name="options">Optional CSV read options to customize parsing behavior.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="DataFusionException">Thrown when table registration fails.</exception>
    public Task RegisterCsvAsync(string tableName, string filePath, CsvReadOptions? options = null)
    {
        using var optionsData = PinnedProtobufData.FromMessage(options?.ToProto());
        
        var (id, tcs) = AsyncOperations.Instance.Create();
        var result = NativeMethods.ContextRegisterCsv(_handle, tableName, filePath, optionsData.ToBytesData(), GenericCallbacks.CallbackForVoidHandle, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start registering CSV file");
        }

        return tcs.Task;
    }
    
    /// <summary>
    /// Registers a JSON file as a table in this session.
    /// </summary>
    /// <param name="tableName">The name to use for the table.</param>
    /// <param name="filePath">The path to the JSON file.</param>
    /// <param name="options">Optional JSON read options to customize parsing behavior.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="DataFusionException">Thrown when table registration fails.</exception>
    public Task RegisterJsonAsync(string tableName, string filePath, JsonReadOptions? options = null)
    {
        using var optionsData = PinnedProtobufData.FromMessage(options?.ToProto());

        var (id, tcs) = AsyncOperations.Instance.Create();
        var result = NativeMethods.ContextRegisterJson(_handle, tableName, filePath, optionsData.ToBytesData(), GenericCallbacks.CallbackForVoidHandle, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start registering JSON file");
        }
        return tcs.Task;
    }
    
    /// <summary>
    /// Registers a Parquet file as a table in this session.
    /// </summary>
    /// <param name="tableName">The name to use for the table.</param>
    /// <param name="filePath">The path to the Parquet file.</param>
    /// <param name="options">Optional Parquet read options to customize reading behavior.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="DataFusionException">Thrown when table registration fails.</exception>
    public Task RegisterParquetAsync(string tableName, string filePath, ParquetReadOptions? options = null)
    {
        using var optionsData = PinnedProtobufData.FromMessage(options?.ToProto());

        var (id, tcs) = AsyncOperations.Instance.Create();
        var result = NativeMethods.ContextRegisterParquet(_handle, tableName, filePath, optionsData.ToBytesData(), GenericCallbacks.CallbackForVoidHandle, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start registering Parquet file");
        }
        return tcs.Task;
    }
    
    /// <summary>
    /// Deregisters a table from this session.
    /// </summary>
    /// <param name="tableName">The name of the table to deregister.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="DataFusionException">Thrown when table deregistration fails.</exception>
    public Task DeregisterTableAsync(string tableName)
    {
        var (id, tcs) = AsyncOperations.Instance.Create();
        var result = NativeMethods.ContextDeregisterTable(_handle, tableName, GenericCallbacks.CallbackForVoidHandle, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start deregistering table");
        }
        return tcs.Task;
    }

    /// <summary>
    /// Executes a SQL query and returns the result as a DataFrame.
    /// </summary>
    /// <param name="sql">The SQL query to execute.</param>
    /// <returns>A task containing the resulting <see cref="DataFrame"/>.</returns>
    /// <exception cref="DataFusionException">Thrown when query execution fails.</exception>
    /// <example>
    /// <code language="csharp">
    /// var df = await session.SqlAsync("SELECT * FROM my_table");
    /// </code>
    /// </example>
    public async Task<DataFrame> SqlAsync(string sql)
    {
        ArgumentNullException.ThrowIfNull(sql);
        
        var (id, tcs) = AsyncOperations.Instance.Create<DataFrameSafeHandle>();
        var result = NativeMethods.ContextSql(_handle, sql, BytesData.Empty, CallbackForSqlAsyncHandle, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start executing SQL query");
        }

        var dataFrameSafeHandle = await tcs.Task.ConfigureAwait(false);
        return new DataFrame(this, dataFrameSafeHandle);
    }
    
    /// <summary>
    /// Executes a SQL query with named parameters and returns the result as a DataFrame.
    /// </summary>
    /// <param name="sql">The SQL query to execute, which can contain named parameter placeholders (e.g., $paramName).</param>
    /// <param name="parameters">A named parameters to bind to the query.</param>
    /// <returns>A task containing the resulting <see cref="DataFrame"/>.</returns>
    /// <exception cref="DataFusionException">Thrown when query execution fails.</exception>
    /// <example>
    /// <code language="csharp">
    /// var df = await session.SqlAsync("SELECT * FROM my_table WHERE id = $id", [("id", 123)]);
    /// </code>
    /// </example>
    public async Task<DataFrame> SqlAsync(string sql, IEnumerable<NamedScalarValueAndMetadata> parameters)
    {
        ArgumentNullException.ThrowIfNull(sql);
        ArgumentNullException.ThrowIfNull(parameters);
        
        Task<DataFrameSafeHandle> task;
        using (var paramValuesData = PinnedProtobufData.FromMessage(parameters.ToProto()))
        {
            var (id, tcs) = AsyncOperations.Instance.Create<DataFrameSafeHandle>();
            var result = NativeMethods.ContextSql(_handle, sql, paramValuesData.ToBytesData(), CallbackForSqlAsyncHandle, id);
            if (result != DataFusionErrorCode.Ok)
            {
                AsyncOperations.Instance.Abort(id);
                throw new DataFusionException(result, "Failed to start executing SQL query");
            }
            
            task = tcs.Task;
        }
        
        var dataFrameSafeHandle = await task.ConfigureAwait(false);
        return new DataFrame(this, dataFrameSafeHandle);
    }
    
    /// <summary>
    /// Registers a local filesystem object store for the given URL.
    /// </summary>
    /// <param name="url">The URL scheme to register (e.g., "file:///").</param>
    /// <exception cref="DataFusionException">Thrown when registration fails (e.g., invalid URL).</exception>
#pragma warning disable CA1054 // URL is passed as-is to DataFusion's native, System.Uri would add redundant conversion.
    public void RegisterLocalFileSystem(string url)
#pragma warning restore CA1054
    {
        ArgumentNullException.ThrowIfNull(url);
        
        var id = SyncOperations.Instance.Create();
        var result = NativeMethods.ContextRegisterObjectStoreLocal(_handle, url, GenericCallbacks.CallbackForVoidSyncHandle, id);
        if (result != DataFusionErrorCode.Ok)
        {
            SyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to register local file system object store");
        }
        SyncOperations.Instance.TakeResult(id);
    }

    /// <summary>
    /// Registers an S3-compatible object store for the given URL.
    /// </summary>
    /// <param name="url">The S3 URL to register (e.g., "s3://my-bucket").</param>
    /// <param name="options">Optional S3 configuration. If null, bucket name is extracted from URL and credentials from environment.</param>
    /// <exception cref="DataFusionException">Thrown when registration fails.</exception>
#pragma warning disable CA1054 // URL is passed as-is to DataFusion's native, System.Uri would add redundant conversion.
    public void RegisterS3ObjectStore(string url, S3ObjectStoreOptions? options = null)
#pragma warning restore CA1054
    {
        ArgumentNullException.ThrowIfNull(url);
        using var optionsData = PinnedProtobufData.FromMessage(options?.ToProto());
        var id = SyncOperations.Instance.Create();
        var result = NativeMethods.ContextRegisterObjectStoreS3(_handle, url, optionsData.ToBytesData(), GenericCallbacks.CallbackForVoidSyncHandle, id);
        if (result != DataFusionErrorCode.Ok)
        {
            SyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to register S3 object store");
        }
        SyncOperations.Instance.TakeResult(id);
    }

    /// <summary>
    /// Registers an Azure Blob Storage object store for the given URL.
    /// </summary>
    /// <param name="url">The Azure URL to register (e.g., "az://my-container").</param>
    /// <param name="options">Optional Azure configuration. If null, container name is extracted from URL and credentials from environment.</param>
    /// <exception cref="DataFusionException">Thrown when registration fails.</exception>
#pragma warning disable CA1054 // URL is passed as-is to DataFusion's native, System.Uri would add redundant conversion.
    public void RegisterAzureBlobStorage(string url, AzureBlobStorageOptions? options = null)
#pragma warning restore CA1054
    {
        ArgumentNullException.ThrowIfNull(url);
        using var optionsData = PinnedProtobufData.FromMessage(options?.ToProto());
        var id = SyncOperations.Instance.Create();
        var result = NativeMethods.ContextRegisterObjectStoreAzure(_handle, url, optionsData.ToBytesData(), GenericCallbacks.CallbackForVoidSyncHandle, id);
        if (result != DataFusionErrorCode.Ok)
        {
            SyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to register Azure Blob Storage object store");
        }
        SyncOperations.Instance.TakeResult(id);
    }

    /// <summary>
    /// Registers a Google Cloud Storage object store for the given URL.
    /// </summary>
    /// <param name="url">The GCS URL to register (e.g., "gs://my-bucket").</param>
    /// <param name="options">Optional GCS configuration. If null, bucket name is extracted from URL and credentials from environment.</param>
    /// <exception cref="DataFusionException">Thrown when registration fails.</exception>
#pragma warning disable CA1054 // URL is passed as-is to DataFusion's native, System.Uri would add redundant conversion.
    public void RegisterGoogleCloudStorage(string url, GoogleCloudStorageOptions? options = null)
#pragma warning restore CA1054
    {
        ArgumentNullException.ThrowIfNull(url);
        using var optionsData = PinnedProtobufData.FromMessage(options?.ToProto());
        var id = SyncOperations.Instance.Create();
        var result = NativeMethods.ContextRegisterObjectStoreGcs(_handle, url, optionsData.ToBytesData(), GenericCallbacks.CallbackForVoidSyncHandle, id);
        if (result != DataFusionErrorCode.Ok)
        {
            SyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to register Google Cloud Storage object store");
        }
        SyncOperations.Instance.TakeResult(id);
    }

    /// <summary>
    /// Registers an HTTP object store for the given URL.
    /// </summary>
    /// <param name="url">The HTTP URL to register (e.g., "https://example.com/data/").</param>
    /// <param name="options">Optional HTTP configuration.</param>
    /// <exception cref="DataFusionException">Thrown when registration fails.</exception>
#pragma warning disable CA1054 // URL is passed as-is to DataFusion's native, System.Uri would add redundant conversion.
    public void RegisterHttpObjectStore(string url, HttpObjectStoreOptions? options = null)
#pragma warning restore CA1054
    {
        ArgumentNullException.ThrowIfNull(url);
        using var optionsData = PinnedProtobufData.FromMessage(options?.ToProto());
        var id = SyncOperations.Instance.Create();
        var result = NativeMethods.ContextRegisterObjectStoreHttp(_handle, url, optionsData.ToBytesData(), GenericCallbacks.CallbackForVoidSyncHandle, id);
        if (result != DataFusionErrorCode.Ok)
        {
            SyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to register HTTP object store");
        }
        SyncOperations.Instance.TakeResult(id);
    }

    /// <summary>
    /// Deregisters an object store for the given URL.
    /// </summary>
    /// <param name="url">The URL of the object store to deregister.</param>
    /// <exception cref="DataFusionException">Thrown when deregistration fails.</exception>
#pragma warning disable CA1054 // URL is passed as-is to DataFusion's native, System.Uri would add redundant conversion.
    public void DeregisterObjectStore(string url)
#pragma warning restore CA1054
    {
        ArgumentNullException.ThrowIfNull(url);
        var id = SyncOperations.Instance.Create();
        var result = NativeMethods.ContextDeregisterObjectStore(_handle, url, GenericCallbacks.CallbackForVoidSyncHandle, id);
        if (result != DataFusionErrorCode.Ok)
        {
            SyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to deregister object store");
        }
        SyncOperations.Instance.TakeResult(id);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _handle.Dispose();
    }
    
    [DataFusionSharpNativeCallback]
    private static void CallbackForSqlAsync(IntPtr result, IntPtr error, ulong handle)
    {
        if (error != IntPtr.Zero)
        {
            var ex = ErrorInfoData.FromIntPtr(error).ToException();
            AsyncOperations.Instance.CompleteWithError<DataFrameSafeHandle>(handle, ex);
            return;
        }

        var dataFrameHandle = Marshal.ReadIntPtr(result);
#pragma warning disable CA2000
        var dataFrameSafeHandle = new DataFrameSafeHandle(dataFrameHandle);
#pragma warning restore CA2000
        AsyncOperations.Instance.CompleteWithResult(handle, dataFrameSafeHandle);
    }
}
