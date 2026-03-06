namespace DataFusionSharp.ObjectStore;

/// <summary>
/// Options for configuring a Google Cloud Storage object store.
/// </summary>
public sealed class GoogleCloudStorageOptions
{
    /// <summary>
    /// GCS bucket name (required).
    /// </summary>
    public required string BucketName { get; set; }

    /// <summary>
    /// GCP project ID. If null, uses environment variable.
    /// </summary>
    public string? ProjectId { get; set; }

    /// <summary>
    /// Path to a service account JSON key file.
    /// </summary>
    public string? CredentialsPath { get; set; }

    /// <summary>
    /// Service account JSON key as a string.
    /// </summary>
    public string? Credentials { get; set; }

    /// <summary>
    /// Service account email.
    /// </summary>
    public string? ServiceAccountEmail { get; set; }

    /// <summary>
    /// Custom endpoint URL.
    /// </summary>
    public string? CustomEndpoint { get; set; }

    /// <summary>
    /// Allow HTTP (non-TLS) connections. Default is false.
    /// </summary>
    public bool? AllowHttp { get; set; }

    /// <summary>
    /// Skip signing requests, for anonymous access. Default is false.
    /// </summary>
    public bool? SkipSignature { get; set; }
}

internal static class ProtoGoogleCloudStorageExtensions
{
    internal static Proto.GoogleCloudStorageOptions ToProto(this GoogleCloudStorageOptions options)
    {
        var proto = new Proto.GoogleCloudStorageOptions { BucketName = options.BucketName };

        if (options.ProjectId is not null)
            proto.ProjectId = options.ProjectId;

        if (options.CredentialsPath is not null)
            proto.CredentialsPath = options.CredentialsPath;

        if (options.Credentials is not null)
            proto.Credentials = options.Credentials;

        if (options.ServiceAccountEmail is not null)
            proto.ServiceAccountEmail = options.ServiceAccountEmail;

        if (options.CustomEndpoint is not null)
            proto.CustomEndpoint = options.CustomEndpoint;

        if (options.AllowHttp is not null)
            proto.AllowHttp = options.AllowHttp.Value;

        if (options.SkipSignature is not null)
            proto.SkipSignature = options.SkipSignature.Value;

        return proto;
    }
}
