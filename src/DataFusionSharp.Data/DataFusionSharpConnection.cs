using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace DataFusionSharp.Data;

/// <summary>
/// Represents a connection to a DataFusion <see cref="SessionContext"/> for ADO.NET operations.
/// </summary>
/// <remarks>
/// Unlike traditional database connections, this connection wraps a live <see cref="SessionContext"/> instance rather than establishing a network connection.
/// <see cref="Open"/> and <see cref="Close"/> only manage the logical connection state; the underlying <see cref="SessionContext"/> lifecycle is managed externally by the caller.
/// Transactions are not supported by DataFusion.
/// </remarks>
public sealed class DataFusionSharpConnection : DbConnection
{
    private readonly SessionContext _sessionContext;
    private readonly bool _leaveOpen;

    private ConnectionState _state = ConnectionState.Closed;

    /// <summary>
    /// Initializes a new <see cref="DataFusionSharpConnection"/> that wraps the given <see cref="SessionContext"/>.
    /// </summary>
    /// <param name="sessionContext">The DataFusion session context to execute queries against. Its lifetime is managed by the caller.</param>
    /// <param name="leaveOpen">If true, disposing this connection will not dispose the underlying <paramref name="sessionContext"/>. Defaults to false.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="sessionContext"/> is null.</exception>
    public DataFusionSharpConnection(SessionContext sessionContext, bool leaveOpen =  false)
    {
        ArgumentNullException.ThrowIfNull(sessionContext);
        _sessionContext = sessionContext;
        _leaveOpen = leaveOpen;
    }

    /// <inheritdoc />
    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) =>
        throw new NotSupportedException("Transactions are not supported by DataFusion.");

    /// <inheritdoc />
    public override void ChangeDatabase(string databaseName) =>
        throw new NotSupportedException("DataFusion does not support switching databases.");

    /// <inheritdoc />
    public override void Close() => _state = ConnectionState.Closed;

    /// <inheritdoc />
    public override void Open() => _state = ConnectionState.Open;

    /// <inheritdoc />
    public override Task OpenAsync(CancellationToken cancellationToken)
    {
        Open();
        return Task.CompletedTask;
    }

    /// <summary>
    /// Gets or sets the connection string. Not meaningful for DataFusion connections; stored as metadata only.
    /// </summary>
    [AllowNull]
    public override string ConnectionString { get; set; } = "Data Source=datafusion;";

    /// <inheritdoc />
    public override string Database => "default";

    /// <inheritdoc />
    public override ConnectionState State => _state;

    /// <inheritdoc />
    public override string DataSource => "DataFusion";

    /// <inheritdoc />
    public override string ServerVersion => "1.0";

    /// <inheritdoc />
    protected override DbCommand CreateDbCommand() => new DataFusionSharpCommand(this);

    /// <summary>
    /// Returns the underlying session context, throwing if the connection is not open.
    /// </summary>
    internal SessionContext GetSessionContext()
    {
        if (_state == ConnectionState.Closed)
            throw new InvalidOperationException("Connection is not open. Call Open() before executing commands.");

        return _sessionContext;
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
        
        if (disposing && !_leaveOpen)
            _sessionContext.Dispose();
    }
}

/// <summary>
/// Extension methods for <see cref="SessionContext"/> to create <see cref="DataFusionSharpConnection"/> instances.
/// </summary>
public static class DataFusionSharpConnectionExtensions
{
    /// <summary>
    /// Creates a new <see cref="DataFusionSharpConnection"/> that wraps the given <see cref="SessionContext"/>.
    /// </summary>
    /// <param name="sessionContext">The DataFusion session context to execute queries against. Its lifetime is managed by the caller.</param>
    /// <param name="leaveOpen">If true, disposing the returned connection will not dispose the underlying <paramref name="sessionContext"/>. Defaults to false.</param>
    /// <returns>A new <see cref="DataFusionSharpConnection"/> instance.</returns>
    public static DataFusionSharpConnection AsConnection(this SessionContext sessionContext, bool leaveOpen = false) =>
        new(sessionContext, leaveOpen);
}