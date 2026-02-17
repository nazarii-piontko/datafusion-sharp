namespace DataFusionSharp.Tests;

internal sealed class TempInputFile : IDisposable
{
    public string Path { get; }

    private TempInputFile(string path)
    {
        Path = path;
    }
    
    ~TempInputFile()
    {
        Cleanup();
    }

    public static async Task<TempInputFile> CreateAsync(string extension, bool forceCreate = false)
    {
        var tempPath = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"datafusion-sharp-test-{Guid.NewGuid():N}{extension}");

        if (!forceCreate)
            return new TempInputFile(tempPath);
        
        try
        {
            await File.WriteAllTextAsync(tempPath, string.Empty);
        }
        catch
        {
            if (File.Exists(tempPath))
            {
                try
                {
                    File.Delete(tempPath);
                }
                catch
                {
                    // Ignore exceptions during cleanup
                }
            }

            throw;
        }

        return new TempInputFile(tempPath);
    }

    public static async Task<TempInputFile> CreateAsync(string extension, IEnumerable<string> lines)
    {
        var tempPath = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"datafusion-sharp-test-{Guid.NewGuid():N}{extension}");
        
        try
        {
            await File.WriteAllLinesAsync(tempPath, lines);
        }
        catch
        {
            if (File.Exists(tempPath))
            {
                try
                {
                    File.Delete(tempPath);
                }
                catch
                {
                    // Ignore exceptions during cleanup
                }
            }

            throw;
        }

        return new TempInputFile(tempPath);
    }

    public void Dispose()
    {
        Cleanup();
        GC.SuppressFinalize(this);
    }
    
    private void Cleanup()
    {
        if (!File.Exists(Path))
            return;
        
        try
        {
            File.Delete(Path);
        }
        catch
        {
            // Ignore exceptions during cleanup
        }
    }
}