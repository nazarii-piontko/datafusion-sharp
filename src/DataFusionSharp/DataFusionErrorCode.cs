namespace DataFusionSharp;

/// <summary>
/// Error codes returned by DataFusion operations.
/// </summary>
public enum DataFusionErrorCode
{
    /// <summary>Operation completed successfully.</summary>
    Ok = 0,
    /// <summary>An unexpected panic occurred in the native library.</summary>
    Panic = 1,
    /// <summary>An invalid argument was provided.</summary>
    InvalidArgument = 2,
    /// <summary>Failed to initialize the Tokio runtime.</summary>
    RuntimeInitializationFailed = 3,
    /// <summary>Failed to shut down the Tokio runtime.</summary>
    RuntimeShutdownFailed = 4,
    /// <summary>Failed to register a table.</summary>
    TableRegistrationFailed = 5,
    /// <summary>An error occurred while executing SQL.</summary>
    SqlError = 6,
    /// <summary>An error occurred during DataFrame operations.</summary>
    DataFrameError = 7,
    /// <summary>An error occurred in the object store.</summary>
    ObjectStoreError = 8,
    /// <summary>The operation was cancelled before it could complete.</summary>
    Canceled = 9,
}
