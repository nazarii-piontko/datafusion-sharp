using System.Runtime.InteropServices;
using DataFusionSharp.Formats.Csv;
using DataFusionSharp.Formats.Json;
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
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="DataFusionException">Thrown when table registration fails.</exception>
    public Task RegisterParquetAsync(string tableName, string filePath)
    {
        var (id, tcs) = AsyncOperations.Instance.Create();
        var result = NativeMethods.ContextRegisterParquet(_handle, tableName, filePath, GenericCallbacks.CallbackForVoidHandle, id);
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
    public async Task<DataFrame> SqlAsync(string sql, IEnumerable<SqlNamedParameter> parameters)
    {
        ArgumentNullException.ThrowIfNull(sql);
        ArgumentNullException.ThrowIfNull(parameters);
        
        var parametersProto = new Proto.SqlParameters();
        foreach (var param in parameters)
            parametersProto.Values.Add(param.Name, param.ProtoValue);

        Task<DataFrameSafeHandle> task;
        using (var sqlParametersData = PinnedProtobufData.FromMessage(parametersProto))
        {
            var (id, tcs) = AsyncOperations.Instance.Create<DataFrameSafeHandle>();
            var result = NativeMethods.ContextSql(_handle, sql, sqlParametersData.ToBytesData(), CallbackForSqlAsyncHandle, id);
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

/// <summary>
/// Represents a named parameter to be passed to a SQL query. The parameter name should match the placeholder used in the SQL string (e.g., $paramName).
/// </summary>
public readonly record struct SqlNamedParameter
{
    /// <summary>
    /// Gets the name of the parameter.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the value of the parameter.
    /// </summary>
    public object? Value { get; }
    
    /// <summary>
    /// Gets the value of the parameter converted to a protobuf ScalarValue.
    /// </summary>
    internal Proto.ScalarValue ProtoValue { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="SqlNamedParameter"/> struct with the specified name and value.
    /// </summary>
    /// <param name="name">The name of the parameter, without the leading '$' symbol.</param>
    /// <param name="value">The value of the parameter. Must be a primitive type or byte array.</param>
    /// <exception cref="ArgumentException">Thrown when the parameter name is invalid or when the value type is unsupported.</exception>
    public SqlNamedParameter(string name, object? value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        if (name.StartsWith('$'))
            throw new ArgumentException("Parameter name should not start with '$' symbol", nameof(name));

        Name = name;
        Value = value;
        ProtoValue = value.ToProtoScalarValue();
    }
    
    /// <summary>
    /// Implicit conversion from a tuple of (string Name, object? Value) to a <see cref="SqlNamedParameter"/>.
    /// </summary>
    /// <param name="tuple">A tuple containing the parameter name and value.</param>
    /// <returns>>A new instance of <see cref="SqlNamedParameter"/> initialized with the provided name and value.</returns>
    /// <exception cref="ArgumentException">Thrown when the parameter name is invalid or when the value type is unsupported.</exception>
    public static implicit operator SqlNamedParameter((string Name, object? Value) tuple) => new(tuple.Name, tuple.Value);

    /// <summary>
    /// Converts a tuple of (string Name, object? Value) to a <see cref="SqlNamedParameter"/>.
    /// </summary>
    /// <param name="tuple">A tuple containing the parameter name and value.</param>
    /// <returns>>A new instance of <see cref="SqlNamedParameter"/> initialized with the provided name and value.</returns>
    /// <exception cref="ArgumentException">Thrown when the parameter name is invalid or when the value type is unsupported.</exception>
    public static SqlNamedParameter ToSqlNamedParameter((string Name, object? Value) tuple) => new(tuple.Name, tuple.Value);
}
