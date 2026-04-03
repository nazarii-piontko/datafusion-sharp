using DataFusionSharp.Interop;

namespace DataFusionSharp.ObjectStore;

/// <summary>
/// Represents an in-memory object store that can be used with the DataFusion runtime.
/// </summary>
public sealed class InMemoryObjectStore : IDisposable
{
    /// <summary>
    /// Gets the DataFusion runtime associated with this in-memory object store.
    /// </summary>
    public DataFusionRuntime Runtime { get; }

    /// <summary>
    /// Gets the handle to the in-memory store.
    /// </summary>
    internal InMemoryStoreSafeHandle Handle { get; }
    
    internal InMemoryObjectStore(DataFusionRuntime runtime, InMemoryStoreSafeHandle inMemoryStoreHandle)
    {
        Runtime = runtime;
        Handle = inMemoryStoreHandle;
    }

    /// <summary>
    /// Puts data into the in-memory store at the specified path. The data is provided as a byte array.
    /// </summary>
    /// <remarks>
    /// The native code will copy the data from the provided byte array, so the caller can safely modify or dispose of the byte array after this method returns.
    /// </remarks>
    /// <param name="path">Path to data</param>
    /// <param name="data">Bytes data to put</param>
    /// <returns>A task that completes when the put operation is finished.</returns>
    /// <exception cref="ArgumentException">Invalid path</exception>
    /// <exception cref="ArgumentNullException">Null data</exception>
    /// <exception cref="DataFusionException">Failed to put object into in-memory store</exception>
    public Task PutAsync(string path, Memory<byte> data)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        if (data.IsEmpty)
            throw new ArgumentException($"{nameof(data)} must not be empty.", nameof(data));
        
        using var pinnedData = PinnedBytesData.FromMemory(data);
        
        var (id, tcs) = AsyncOperations.Instance.Create();
        var result = NativeMethods.InMemoryStorePut(Handle, path, pinnedData.ToBytesData(), true, GenericCallbacks.CallbackForVoidHandle, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to put object into in-memory store.");
        }
        
        return tcs.Task;
    }

    /// <summary>
    /// Deletes data at the specified path from the in-memory store.
    /// </summary>
    /// <param name="path">Path to data to delete</param>
    /// <returns>A task that completes when the delete operation is finished.</returns>
    /// <exception cref="ArgumentException">Invalid path</exception>
    /// <exception cref="ArgumentNullException">Null data</exception>
    /// <exception cref="DataFusionException">Failed to delete object from in-memory store</exception>
    public Task DeleteAsync(string path)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        
        var (id, tcs) = AsyncOperations.Instance.Create();
        var result = NativeMethods.InMemoryStoreDelete(Handle, path, GenericCallbacks.CallbackForVoidHandle, id);
        if (result != DataFusionErrorCode.Ok)
        {
            AsyncOperations.Instance.Abort(id);
            throw new DataFusionException(result, "Failed to delete object from in-memory store.");
        }
        
        return tcs.Task;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        Handle.Dispose();
    }
}