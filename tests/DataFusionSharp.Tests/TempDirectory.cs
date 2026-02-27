namespace DataFusionSharp.Tests;

internal sealed class TempDirectory : IDisposable
{
    public string Path { get; }

    private TempDirectory(string path)
    {
        Path = path;
    }

    ~TempDirectory()
    {
        Cleanup();
    }

    public static TempDirectory Create(string prefix = "datafusion-sharp-test")
    {
        var path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"{prefix}-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return new TempDirectory(path);
    }

    public void Dispose()
    {
        Cleanup();
        GC.SuppressFinalize(this);
    }

    private void Cleanup()
    {
        if (!Directory.Exists(Path))
            return;

        try
        {
            Directory.Delete(Path, recursive: true);
        }
        catch
        {
            // Ignore exceptions during cleanup
        }
    }
}

