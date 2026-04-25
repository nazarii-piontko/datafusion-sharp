using System.Runtime.InteropServices;

namespace DataFusionSharp.Interop;

internal abstract class SyncOperation
{
#if MEMORY_TEST
    private static long _liveInstances;
    internal static long LiveInstances => Interlocked.Read(ref _liveInstances);
#endif

    private GCHandle _handle;

    internal IntPtr GetHandle()
    {
        if (!_handle.IsAllocated)
        {
            _handle = GCHandle.Alloc(this, GCHandleType.Normal);
#if MEMORY_TEST
            Interlocked.Increment(ref _liveInstances);
#endif
        }

        return GCHandle.ToIntPtr(_handle);
    }
    
    protected void Cleanup()
    {
        if (_handle.IsAllocated)
        {
            try
            {
                _handle.Free();
#if MEMORY_TEST
                Interlocked.Decrement(ref _liveInstances);
#endif
            }
            catch (InvalidOperationException)
            {
                // Handle was already freed, ignore
            }
        }
    }
}

internal sealed class SyncVoidOperation : SyncOperation
{
    private Exception? _exception;

    internal void EnsureNativeCall(DataFusionErrorCode result, string errorMessage)
    {
        Cleanup();

        if (result != DataFusionErrorCode.Ok)
            throw new DataFusionException(result, errorMessage);
        
        if (_exception is not null)
            throw _exception;
    }

    internal void Complete(Exception? exception = null)
    {
        _exception = exception;
    }
    
    internal static SyncVoidOperation? FromHandle(IntPtr handle)
    {
        try
        {
            var h = GCHandle.FromIntPtr(handle);
            if (!h.IsAllocated)
                return null;

            return h.Target as SyncVoidOperation;
        }
        catch (InvalidOperationException)
        {
            // The handle was freed between the IsAllocated check and the Target access.
            // Or the handle is invalid. In either case, we can just return null.
            return null;
        }
    }
}

internal sealed class SyncOperation<TResult> : SyncOperation
{
    private TResult _result = default!;
    private Exception? _exception;

    internal TResult EnsureNativeCall(DataFusionErrorCode result, string errorMessage)
    {
        Cleanup();

        if (result != DataFusionErrorCode.Ok)
            throw new DataFusionException(result, errorMessage);
        
        if (_exception is not null)
            throw _exception;

        return _result;
    }

    internal void Complete(TResult result)
    {
        _result = result;
    }
    
    internal void Complete(Exception exception)
    {
        _exception = exception;
    }
    
    internal static SyncOperation<TResult>? FromHandle(IntPtr handle)
    {
        try
        {
            var h = GCHandle.FromIntPtr(handle);
            if (!h.IsAllocated)
                return null;

            return h.Target as SyncOperation<TResult>;
        }
        catch (InvalidOperationException)
        {
            // The handle was freed between the IsAllocated check and the Target access.
            // Or the handle is invalid. In either case, we can just return null.
            return null;
        }
    }
}
