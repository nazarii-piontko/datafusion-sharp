using DataFusionSharp.Interop;

namespace DataFusionSharp;

public static class DataFusion
{
    private static bool _initialized;

    public static bool IsInitialized => _initialized;

    public static void Initialize(uint workerThreads = 0, uint maxBlockingThreads = 0)
    {
        if (_initialized)
            return;

        var result = NativeMethods.Init(workerThreads, maxBlockingThreads);

        if (result == ErrorCode.AlreadyInitialized)
        {
            _initialized = true;
            return;
        }

        if (result != ErrorCode.Ok)
            throw new DataFusionException($"Failed to initialize DataFusion: {result}");

        _initialized = true;
    }

    public static void Shutdown(ulong timeoutMillis = 5000)
    {
        if (!_initialized)
            return;

        var result = NativeMethods.Shutdown(timeoutMillis);

        if (result != ErrorCode.Ok)
            throw new DataFusionException($"Failed to shutdown DataFusion: {result}");

        _initialized = false;
    }
}
