namespace DataFusionSharp.ObjectStore;

/// <summary>
/// Options for configuring an HTTP object store.
/// </summary>
public sealed class HttpObjectStoreOptions
{
    /// <summary>
    /// Base URL of the HTTP server (required).
    /// </summary>
#pragma warning disable CA1056 // URL is passed as-is to DataFusion's native, System.Uri would add redundant conversion.
    public required string Url { get; set; }
#pragma warning restore CA1056

    /// <summary>
    /// Allow HTTP (non-TLS) connections. Default is false.
    /// </summary>
    public bool? AllowHttp { get; set; }

    /// <summary>
    /// Allow invalid TLS certificates. Default is false.
    /// </summary>
    public bool? AllowInvalidCertificates { get; set; }

    /// <summary>
    /// Custom HTTP headers (e.g., Authorization, API keys).
    /// </summary>
    public Dictionary<string, string>? CustomHeaders { get; init; }
}

internal static class ProtoHttpObjectStoreExtensions
{
    internal static Proto.HttpObjectStoreOptions ToProto(this HttpObjectStoreOptions options)
    {
        var proto = new Proto.HttpObjectStoreOptions { Url = options.Url };

        if (options.AllowHttp is not null)
            proto.AllowHttp = options.AllowHttp.Value;

        if (options.AllowInvalidCertificates is not null)
            proto.AllowInvalidCertificates = options.AllowInvalidCertificates.Value;

        if (options.CustomHeaders is not null)
        {
            foreach (var (key, value) in options.CustomHeaders)
                proto.CustomHeaders.Add(key, value);
        }

        return proto;
    }
}
