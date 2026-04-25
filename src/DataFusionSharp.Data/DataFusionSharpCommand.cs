using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Text.RegularExpressions;

namespace DataFusionSharp.Data;

/// <summary>
/// Represents a SQL command that executes against a <see cref="DataFusionSharpConnection"/>.
/// </summary>
/// <remarks>
/// <para>
/// DataFusion uses <c>$paramName</c> as its native parameter placeholder syntax.
/// For convenience this command automatically translates the standard ADO.NET <c>@paramName</c> syntax used by tools such as Dapper into the <c>$paramName</c> format before executing the query.
/// </para>
/// <para>
/// <see cref="ExecuteNonQuery"/> and <see cref="ExecuteNonQueryAsync"/> always return <c>-1</c> because DataFusion is an analytical engine and does not track affected row counts.
/// </para>
/// </remarks>
public sealed class DataFusionSharpCommand : DbCommand
{
    // Matches @identifier placeholders and replaces them with $identifier for DataFusion.
    private static readonly Regex AtParamRegex = new(@"@(\w+)", RegexOptions.Compiled, TimeSpan.FromSeconds(1));

    private DataFusionSharpConnection? _connection;
    private readonly DataFusionSharpParameterCollection _parameters = new();

    /// <summary>
    /// Initializes a new command.
    /// </summary>
    public DataFusionSharpCommand()
    {
    }
    
    /// <summary>
    /// Initializes a new command bound to <paramref name="connection"/>.
    /// </summary>
    public DataFusionSharpCommand(DataFusionSharpConnection connection)
    {
        ArgumentNullException.ThrowIfNull(connection);
        
        _connection = connection;
    }

    /// <inheritdoc />
    [AllowNull]
    public override string CommandText { get; set; } = string.Empty;

    /// <inheritdoc />
    public override int CommandTimeout { get; set; } = 30;

    /// <inheritdoc />
    public override CommandType CommandType
    {
        get => CommandType.Text;
        set
        {
            if (value != CommandType.Text) 
                throw new NotSupportedException("DataFusion only supports CommandType.Text.");
        }
    }

    /// <inheritdoc />
    public override bool DesignTimeVisible { get; set; }

    /// <inheritdoc />
    public override UpdateRowSource UpdatedRowSource
    {
        get => UpdateRowSource.Both;
        set
        {
            if (value != UpdateRowSource.Both)
                throw new NotSupportedException("DataFusion only supports UpdateRowSource.Both.");
        }
    }

    /// <inheritdoc />
    protected override DbConnection? DbConnection
    {
        get => _connection;
        set => _connection = (DataFusionSharpConnection?)value;
    }

    /// <inheritdoc />
    protected override DbParameterCollection DbParameterCollection => _parameters;

    /// <inheritdoc />
    protected override DbTransaction? DbTransaction
    {
        get => null;
        set
        {
            if (value is not null)
                throw new NotSupportedException("DataFusion does not support transactions.");
        }
    }

    /// <inheritdoc />
    public override void Cancel() => throw new NotSupportedException("DataFusion does not support cancel.");

    /// <inheritdoc />
    /// <returns>Always <c>-1</c>; DataFusion does not track affected row counts.</returns>
    public override int ExecuteNonQuery() => ExecuteNonQueryAsync(CancellationToken.None).GetAwaiter().GetResult();

    /// <inheritdoc />
    /// <returns>Always <c>-1</c>; DataFusion does not track affected row counts.</returns>
    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        using var df = await ExecuteDataFrameAsync().ConfigureAwait(false);
        using var stream = await df.ExecuteStreamAsync(cancellationToken).ConfigureAwait(false);
        await foreach (var batch in stream.WithCancellation(cancellationToken).ConfigureAwait(false))
            batch.Dispose();
        return -1;
    }

    /// <inheritdoc />
    public override object? ExecuteScalar() => ExecuteScalarAsync(CancellationToken.None).GetAwaiter().GetResult();

    /// <inheritdoc />
    public override async Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
        using var reader = (DataFusionSharpDataReader) await ExecuteDbDataReaderAsync(CommandBehavior.Default, cancellationToken).ConfigureAwait(false);

        if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false) && reader.FieldCount > 0)
        {
            return await reader.IsDBNullAsync(0, cancellationToken).ConfigureAwait(false)
                ? DBNull.Value
                : reader.GetValue(0);
        }

        return null;
    }

    /// <inheritdoc />
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        return ExecuteDbDataReaderAsync(behavior, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <inheritdoc />
    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        var df = await ExecuteDataFrameAsync().ConfigureAwait(false);
        try
        {
            var stream = await df.ExecuteStreamAsync(cancellationToken).ConfigureAwait(false);
            return new DataFusionSharpDataReader(df, stream);
        }
        catch
        {
            df.Dispose();
            throw;
        }
    }

    /// <inheritdoc />
    public override void Prepare() { /* no-op – DataFusion has no explicit prepare step */ }

    /// <inheritdoc />
    protected override DbParameter CreateDbParameter() => new DataFusionSharpParameter();

    private SessionContext GetSession()
    {
        if (_connection is null)
            throw new InvalidOperationException("The command has no associated connection. Set the Connection property before executing.");

        return _connection.GetSessionContext();
    }

    private Task<DataFrame> ExecuteDataFrameAsync()
    {
        var session = GetSession();
        var sql = TranslateCommandText(CommandText);
        
        return _parameters.Count > 0
            ? session.SqlAsync(sql, _parameters.ToDataFusionParameters())
            : session.SqlAsync(sql);
    }

    /// <summary>
    /// Translates <c>@param</c> placeholders in <paramref name="sql"/> to DataFusion's native <c>$param</c> syntax.
    /// </summary>
    private static string TranslateCommandText(string sql) => AtParamRegex.Replace(sql, "$$$1");
}

