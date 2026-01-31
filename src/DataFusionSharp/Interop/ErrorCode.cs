namespace DataFusionSharp.Interop;

internal enum ErrorCode
{
    Ok = 0,
    Panic = 1,
    LockPoisoned = 2,
    NotInitialized = 3,
    AlreadyInitialized = 4,
    RuntimeError = 5,
    NullPointer = 6,
    InvalidUtf8 = 7,
    DataFusionError = 8,
}
