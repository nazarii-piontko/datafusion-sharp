using DataFusionSharp.Formats.Csv;
using DataFusionSharp.Interop;

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
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="DataFusionException">Thrown when table registration fails.</exception>
    public Task RegisterCsvAsync(string tableName, string filePath, CsvReadOptions? options = null)
    {
        using var optionsData = PinnedProtobufData.FromMessage(options?.ToProto());
        
        var (id, tcs) = AsyncOperations.Instance.Create();
        var result = NativeMethods.ContextRegisterCsv(_handle.GetHandle(), tableName, filePath, optionsData.ToBytesData(), AsyncOperationGenericCallbacks.VoidResultHandler, id);
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
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="DataFusionException">Thrown when table registration fails.</exception>
    public Task RegisterJsonAsync(string tableName, string filePath)
    {
        var (id, tcs) = AsyncOperations.Instance.Create();
        var result = NativeMethods.ContextRegisterJson(_handle.GetHandle(), tableName, filePath, AsyncOperationGenericCallbacks.VoidResultHandler, id);
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
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="DataFusionException">Thrown when table registration fails.</exception>
    public Task RegisterParquetAsync(string tableName, string filePath)
    {
        var (id, tcs) = AsyncOperations.Instance.Create();
        var result = NativeMethods.ContextRegisterParquet(_handle.GetHandle(), tableName, filePath, AsyncOperationGenericCallbacks.VoidResultHandler, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start registering Parquet file");
        }
        return tcs.Task;
    }
    
    /// <summary>
    /// Executes a SQL query and returns the result as a DataFrame.
    /// </summary>
    /// <param name="sql">The SQL query to execute.</param>
    /// <returns>A task containing the resulting <see cref="DataFrame"/>.</returns>
    /// <exception cref="DataFusionException">Thrown when query execution fails.</exception>
    public async Task<DataFrame> SqlAsync(string sql)
    {
        var (id, tcs) = AsyncOperations.Instance.Create<IntPtr>();
        var result = NativeMethods.ContextSql(_handle.GetHandle(), sql, AsyncOperationGenericCallbacks.IntPtrResultHandler, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to start executing SQL query");
        }
        
        var dataFrameHandle = await tcs.Task.ConfigureAwait(false);

        return new DataFrame(this, new DataFrameSafeHandle(dataFrameHandle));
    }
    
    /// <inheritdoc />
    public void Dispose()
    {
        _handle.Dispose();
    }
}