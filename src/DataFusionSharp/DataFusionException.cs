using System.Diagnostics;

namespace DataFusionSharp;

/// <summary>
/// Exception thrown when a DataFusion operation fails. Contains an <see cref="ErrorCode"/> indicating the failure category.
/// </summary>
public class DataFusionException : Exception
{
    /// <summary>
    /// Gets the error code associated with this exception.
    /// </summary>
    public DataFusionErrorCode ErrorCode { get; }
    
    /// <summary>
    /// Initializes a new instance of the <see cref="DataFusionException"/> class with a default message and error code.
    /// </summary>
    public DataFusionException()
        : base("An unknown DataFusion error occurred.")
    {
        ErrorCode = DataFusionErrorCode.Panic;
    }

    /// <summary>
    /// Initializes a new instance with the specified error code and a default message.
    /// </summary>
    /// <param name="errorCode">The error code.</param>
    public DataFusionException(DataFusionErrorCode errorCode)
        : base($"DataFusion error occurred: {errorCode}")
    {
        ErrorCode = errorCode;
    }

    /// <summary>
    /// Initializes a new instance with the specified error code and message.
    /// </summary>
    /// <param name="errorCode">The error code.</param>
    /// <param name="message">The error message.</param>
    public DataFusionException(DataFusionErrorCode errorCode, string message)
        : base(message)
    {
        ErrorCode = errorCode;
    }

    /// <summary>
    /// Initializes a new instance with the specified error code, message, and inner exception.
    /// </summary>
    /// <param name="errorCode">The error code.</param>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    public DataFusionException(DataFusionErrorCode errorCode, string message, Exception innerException)
        : base(message, innerException)
    {
        ErrorCode = errorCode;
    }
    
    /// <summary>
    /// Throws a <see cref="DataFusionException"/> if the provided <paramref name="errorCode"/> indicates an error (i.e., is not <see cref="DataFusionErrorCode.Ok"/>).
    /// </summary>
    /// <param name="errorCode">The error code to check.</param>
    /// <param name="message">The message to include in the exception if an error is detected.</param>
    /// <exception cref="DataFusionException">Thrown when <paramref name="errorCode"/> is not <see cref="DataFusionErrorCode.Ok"/>.</exception>
    [StackTraceHidden]
    internal static void ThrowIfError(DataFusionErrorCode errorCode, string message)
    {
        if (errorCode != DataFusionErrorCode.Ok)
            throw new DataFusionException(errorCode, message);
    }
}
