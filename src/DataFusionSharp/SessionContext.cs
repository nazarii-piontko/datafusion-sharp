using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
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
public sealed class SessionContext : IDisposable
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
    /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="DataFusionException">Thrown when table registration fails.</exception>
    public Task RegisterCsvAsync(string tableName, string filePath, CsvReadOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tableName);
        ArgumentNullException.ThrowIfNull(filePath);
        
        using var optionsData = PinnedBytesData.FromMessage(options?.ToProto());

        unsafe
        {
            var op = new AsyncVoidOperation(cancellationToken);
            var result = NativeMethods.ContextRegisterCsv(_handle, tableName, filePath, optionsData.ToBytesData(), &GenericCallbacks.CallbackForVoid, op.GetHandle());
            op.EnsureNativeCall(result, "Failed to start CSV file registration.");

            return op.Task;
        }
    }

    /// <summary>
    /// Registers a JSON file as a table in this session.
    /// </summary>
    /// <param name="tableName">The name to use for the table.</param>
    /// <param name="filePath">The path to the JSON file.</param>
    /// <param name="options">Optional JSON read options to customize parsing behavior.</param>
    /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="DataFusionException">Thrown when table registration fails.</exception>
    public Task RegisterJsonAsync(string tableName, string filePath, JsonReadOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tableName);
        ArgumentNullException.ThrowIfNull(filePath);
        
        using var optionsData = PinnedBytesData.FromMessage(options?.ToProto());

        unsafe
        {
            var op = new AsyncVoidOperation(cancellationToken);
            var result = NativeMethods.ContextRegisterJson(_handle, tableName, filePath, optionsData.ToBytesData(), &GenericCallbacks.CallbackForVoid, op.GetHandle());
            op.EnsureNativeCall(result, "Failed to start JSON file registration.");

            return op.Task;
        }
    }

    /// <summary>
    /// Registers a Parquet file as a table in this session.
    /// </summary>
    /// <param name="tableName">The name to use for the table.</param>
    /// <param name="filePath">The path to the Parquet file.</param>
    /// <param name="options">Optional Parquet read options to customize reading behavior.</param>
    /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="DataFusionException">Thrown when table registration fails.</exception>
    public Task RegisterParquetAsync(string tableName, string filePath, ParquetReadOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tableName);
        ArgumentNullException.ThrowIfNull(filePath);
        
        using var optionsData = PinnedBytesData.FromMessage(options?.ToProto());

        unsafe
        {
            var op = new AsyncVoidOperation(cancellationToken);
            var result = NativeMethods.ContextRegisterParquet(_handle, tableName, filePath, optionsData.ToBytesData(), &GenericCallbacks.CallbackForVoid, op.GetHandle());
            op.EnsureNativeCall(result, "Failed to start Parquet file registration.");

            return op.Task;
        }
    }

    /// <summary>
    /// Registers an in-memory Arrow RecordBatch as a table in this session.
    /// </summary>
    /// <remarks>
    /// It uses Arrow IPC format to transfer the RecordBatch to the native side.
    /// </remarks>
    /// <param name="tableName">The name to use for the table.</param>
    /// <param name="batch">The Arrow RecordBatch to register as a table.</param>
    /// <exception cref="DataFusionException">Thrown when table registration fails.</exception>
    public void RegisterBatch(string tableName, RecordBatch batch)
    {
        ArgumentNullException.ThrowIfNull(tableName);
        ArgumentNullException.ThrowIfNull(batch);
        
        using var memoryStream = new MemoryStream();
        using (var writer = new Apache.Arrow.Ipc.ArrowStreamWriter(memoryStream, batch.Schema, true))
        {
            writer.WriteRecordBatch(batch);
            writer.WriteEnd();
        }
        using var memoryHandle = memoryStream.GetBuffer().AsMemory().Pin();

        unsafe
        {
            var op = new SyncVoidOperation();
            var result = NativeMethods.ContextRegisterBatch(_handle, tableName, BytesData.FromPinned(memoryHandle, (int)memoryStream.Length), &GenericCallbacks.CallbackForVoidSync, op.GetHandle());
            op.EnsureNativeCall(result, "Failed to start record batch registration.");
        }
    }

    /// <summary>
    /// Deregisters a table from this session.
    /// </summary>
    /// <param name="tableName">The name of the table to deregister.</param>
    /// <param name="cancellationToken">An optional cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="DataFusionException">Thrown when table deregistration fails.</exception>
    public Task DeregisterTableAsync(string tableName, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tableName);

        unsafe
        {
            var op = new AsyncVoidOperation(cancellationToken);
            var result = NativeMethods.ContextDeregisterTable(_handle, tableName, &GenericCallbacks.CallbackForVoid, op.GetHandle());
            op.EnsureNativeCall(result, "Failed to start table deregistration.");

            return op.Task;
        }
    }

    /// <summary>
    /// Executes a SQL query and returns the result as a DataFrame.
    /// </summary>
    /// <param name="sql">The SQL query to execute.</param>
    /// <param name="cancellationToken">An optional cancellation token to cancel the query execution.</param>
    /// <returns>A task containing the resulting <see cref="DataFrame"/>.</returns>
    /// <exception cref="DataFusionException">Thrown when query execution fails.</exception>
    /// <example>
    /// <code language="csharp">
    /// var df = await session.SqlAsync("SELECT * FROM my_table");
    /// </code>
    /// </example>
    public async Task<DataFrame> SqlAsync(string sql, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sql);
        
        Task<DataFrameSafeHandle> sqlTask;

        unsafe
        {
            var op = new AsyncOperation<DataFrameSafeHandle>(cancellationToken);
            var result = NativeMethods.ContextSql(_handle, sql, BytesData.Empty, &CallbackForSqlAsync, op.GetHandle());
            op.EnsureNativeCall(result, "Failed to start executing SQL query.");
            sqlTask = op.Task;
        }
        
        var dataFrameSafeHandle = await sqlTask.ConfigureAwait(false);

        return new DataFrame(this, dataFrameSafeHandle);
    }
    
    /// <summary>
    /// Executes a SQL query with named parameters and returns the result as a DataFrame.
    /// </summary>
    /// <param name="sql">The SQL query to execute, which can contain named parameter placeholders (e.g., $paramName).</param>
    /// <param name="parameters">A named parameters to bind to the query.</param>
    /// <param name="cancellationToken">An optional cancellation token to cancel the query execution.</param>
    /// <returns>A task containing the resulting <see cref="DataFrame"/>.</returns>
    /// <exception cref="DataFusionException">Thrown when query execution fails.</exception>
    /// <example>
    /// <code language="csharp">
    /// var df = await session.SqlAsync("SELECT * FROM my_table WHERE id = $id", [("id", 123)]);
    /// </code>
    /// </example>
    public async Task<DataFrame> SqlAsync(string sql, IEnumerable<NamedScalarValueAndMetadata> parameters, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sql);
        ArgumentNullException.ThrowIfNull(parameters);
        
        Task<DataFrameSafeHandle> task;
        using (var paramValuesData = PinnedBytesData.FromMessage(parameters.ToProto()))
        {
            unsafe
            {
                var op = new AsyncOperation<DataFrameSafeHandle>(cancellationToken);
                var result = NativeMethods.ContextSql(_handle, sql, paramValuesData.ToBytesData(), &CallbackForSqlAsync, op.GetHandle());
                op.EnsureNativeCall(result, "Failed to start executing SQL query.");
                task = op.Task;
            }
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

        unsafe
        {
            var op = new SyncVoidOperation();
            var result = NativeMethods.ContextRegisterObjectStoreLocal(_handle, url, &GenericCallbacks.CallbackForVoidSync, op.GetHandle());
            op.EnsureNativeCall(result, "Failed to start local file system object store registration.");
        }
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
        
        using var optionsData = PinnedBytesData.FromMessage(options?.ToProto());

        unsafe
        {
            var op = new SyncVoidOperation();
            var result = NativeMethods.ContextRegisterObjectStoreS3(_handle, url, optionsData.ToBytesData(), &GenericCallbacks.CallbackForVoidSync, op.GetHandle());
            op.EnsureNativeCall(result, "Failed to start S3 object store registration.");
        }
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
        
        using var optionsData = PinnedBytesData.FromMessage(options?.ToProto());

        unsafe
        {
            var op = new SyncVoidOperation();
            var result = NativeMethods.ContextRegisterObjectStoreAzure(_handle, url, optionsData.ToBytesData(), &GenericCallbacks.CallbackForVoidSync, op.GetHandle());
            op.EnsureNativeCall(result, "Failed to start Azure Blob Storage object store registration.");
        }
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
        
        using var optionsData = PinnedBytesData.FromMessage(options?.ToProto());

        unsafe
        {
            var op = new SyncVoidOperation();
            var result = NativeMethods.ContextRegisterObjectStoreGcs(_handle, url, optionsData.ToBytesData(), &GenericCallbacks.CallbackForVoidSync, op.GetHandle());
            op.EnsureNativeCall(result, "Failed to start Google Cloud Storage object store registration.");
        }
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
        
        using var optionsData = PinnedBytesData.FromMessage(options?.ToProto());

        unsafe
        {
            var op = new SyncVoidOperation();
            var result = NativeMethods.ContextRegisterObjectStoreHttp(_handle, url, optionsData.ToBytesData(), &GenericCallbacks.CallbackForVoidSync, op.GetHandle());
            op.EnsureNativeCall(result, "Failed to start HTTP object store registration.");
        }
    }
    
    /// <summary>
    /// Registers an in-memory object store for the given URL.
    /// </summary>
    /// <param name="url">The URL scheme to register (e.g., "memory://").</param>
    /// <param name="store">The in-memory object store instance to register.</param>
    /// <exception cref="ArgumentNullException">Null data</exception>
    /// <exception cref="DataFusionException">Thrown when registration fails.</exception>
#pragma warning disable CA1054 // URL is passed as-is to DataFusion's native, System.Uri would add redundant conversion.
    public void RegisterInMemoryObjectStore(string url, InMemoryObjectStore store)
#pragma warning restore CA1054
    {
        ArgumentNullException.ThrowIfNull(url);
        ArgumentNullException.ThrowIfNull(store);

        unsafe
        {
            var op = new SyncVoidOperation();
            var result = NativeMethods.ContextRegisterObjectStoreInMemory(_handle, url, store.Handle, &GenericCallbacks.CallbackForVoidSync, op.GetHandle());
            op.EnsureNativeCall(result, "Failed to start in-memory object store registration.");
        }
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

        unsafe
        {
            var op = new SyncVoidOperation();
            var result = NativeMethods.ContextDeregisterObjectStore(_handle, url, &GenericCallbacks.CallbackForVoidSync, op.GetHandle());
            op.EnsureNativeCall(result, "Failed to start object store deregistration.");
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _handle.Dispose();
    }
    
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void CallbackForSqlAsync(IntPtr result, IntPtr error, IntPtr handle)
    {
        var op = AsyncOperation<DataFrameSafeHandle>.FromHandle(handle);

        if (error != IntPtr.Zero)
        {
            if (op is null)
                return;

            var ex = ErrorInfoData.FromIntPtr(error).ToException();
            op.Complete(ex);
            return;
        }

        var dataFrameHandle = Marshal.ReadIntPtr(result);
#pragma warning disable CA2000
        var dataFrameSafeHandle = new DataFrameSafeHandle(dataFrameHandle);
#pragma warning restore CA2000
        
        if (op is null)
            dataFrameSafeHandle.Dispose(); // Clean up the native handle if we can't complete the operation
        else
            op.Complete(dataFrameSafeHandle);
    }
}
