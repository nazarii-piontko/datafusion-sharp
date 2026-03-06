namespace DataFusionSharp.ObjectStore;

/// <summary>
/// Options for configuring an Azure Blob Storage object store.
/// </summary>
public sealed class AzureBlobStorageOptions
{
    /// <summary>
    /// Azure container name (required).
    /// </summary>
    public required string ContainerName { get; set; }

    /// <summary>
    /// Azure storage account name. If null, uses environment variable.
    /// </summary>
    public string? AccountName { get; set; }

    /// <summary>
    /// Azure storage access key.
    /// </summary>
    public string? AccessKey { get; set; }

    /// <summary>
    /// Bearer token for authentication.
    /// </summary>
    public string? BearerToken { get; set; }

    /// <summary>
    /// Client ID for service principal authentication.
    /// </summary>
    public string? ClientId { get; set; }

    /// <summary>
    /// Client secret for service principal authentication.
    /// </summary>
    public string? ClientSecret { get; set; }

    /// <summary>
    /// Tenant ID for service principal authentication.
    /// </summary>
    public string? TenantId { get; set; }

    /// <summary>
    /// Shared Access Signature key.
    /// </summary>
    public string? SasKey { get; set; }

    /// <summary>
    /// Custom endpoint URL.
    /// </summary>
    public string? Endpoint { get; set; }

    /// <summary>
    /// Use Azurite emulator. Default is false.
    /// </summary>
    public bool? UseEmulator { get; set; }

    /// <summary>
    /// Allow HTTP (non-TLS) connections. Default is false.
    /// </summary>
    public bool? AllowHttp { get; set; }

    /// <summary>
    /// Skip signing requests, for anonymous access. Default is false.
    /// </summary>
    public bool? SkipSignature { get; set; }
}

internal static class ProtoAzureBlobStorageExtensions
{
    internal static Proto.AzureBlobStorageOptions ToProto(this AzureBlobStorageOptions options)
    {
        var proto = new Proto.AzureBlobStorageOptions { ContainerName = options.ContainerName };

        if (options.AccountName is not null)
            proto.AccountName = options.AccountName;

        if (options.AccessKey is not null)
            proto.AccessKey = options.AccessKey;

        if (options.BearerToken is not null)
            proto.BearerToken = options.BearerToken;

        if (options.ClientId is not null)
            proto.ClientId = options.ClientId;

        if (options.ClientSecret is not null)
            proto.ClientSecret = options.ClientSecret;

        if (options.TenantId is not null)
            proto.TenantId = options.TenantId;

        if (options.SasKey is not null)
            proto.SasKey = options.SasKey;

        if (options.Endpoint is not null)
            proto.Endpoint = options.Endpoint;

        if (options.UseEmulator is not null)
            proto.UseEmulator = options.UseEmulator.Value;

        if (options.AllowHttp is not null)
            proto.AllowHttp = options.AllowHttp.Value;

        if (options.SkipSignature is not null)
            proto.SkipSignature = options.SkipSignature.Value;

        return proto;
    }
}
