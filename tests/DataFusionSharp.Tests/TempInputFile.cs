using System.IO.Compression;

namespace DataFusionSharp.Tests;

internal sealed class TempInputFile : IDisposable
{
    private readonly bool _gzip;
    
    public string Path { get; }

    private TempInputFile(string path, bool gzip)
    {
        Path = path;
        _gzip = gzip;
    }
    
    ~TempInputFile()
    {
        Cleanup();
    }

    public IReadOnlyList<string> ReadLines()
    {
        if (!_gzip)
            return File.ReadAllLines(Path);

        using var fileStream = new FileStream(Path, FileMode.Open, FileAccess.Read, FileShare.Read);
        using var gzipStream = new GZipStream(fileStream, CompressionMode.Decompress);
        using var reader = new StreamReader(gzipStream);
        var lines = new List<string>();
        while (reader.ReadLine() is { } line)
            lines.Add(line);
        return lines;
    }

    public static async Task<TempInputFile> CreateAsync(string extension, bool forceCreate = false, bool gzip = false)
    {
        var tempPath = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"datafusion-sharp-test-{Guid.NewGuid():N}{extension}");

        if (!forceCreate)
            return new TempInputFile(tempPath, gzip);
        
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

        return new TempInputFile(tempPath, gzip);
    }

    public static async Task<TempInputFile> CreateAsync(string extension, IEnumerable<string> lines, bool gzip = false)
    {
        var tempPath = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"datafusion-sharp-test-{Guid.NewGuid():N}{extension}");
        
        try
        {
            if (gzip)
            {
                await using var fileStream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None);
                await using var gzipStream = new GZipStream(fileStream, CompressionLevel.Optimal);
                await using var writer = new StreamWriter(gzipStream);
                
                foreach (var line in lines)
                    await writer.WriteLineAsync(line);
            }
            else
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

        return new TempInputFile(tempPath, gzip);
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