namespace DataFusionSharp.ObjectStore;

/// <summary>
/// Options for configuring an S3-compatible object store.
/// </summary>
public sealed class S3ObjectStoreOptions
{
    /// <summary>
    /// S3 bucket name (required).
    /// </summary>
    public required string BucketName { get; set; }

    /// <summary>
    /// AWS region (e.g., "us-east-1"). If null, uses environment variable or default.
    /// </summary>
    public string? Region { get; set; }

    /// <summary>
    /// AWS access key ID. If null, uses environment variable or IAM credentials.
    /// </summary>
    public string? AccessKeyId { get; set; }

    /// <summary>
    /// AWS secret access key. If null, uses environment variable or IAM credentials.
    /// </summary>
    public string? SecretAccessKey { get; set; }

    /// <summary>
    /// Custom endpoint URL for S3-compatible services (e.g., MinIO).
    /// </summary>
    public string? Endpoint { get; set; }

    /// <summary>
    /// Session token for temporary credentials.
    /// </summary>
    public string? Token { get; set; }

    /// <summary>
    /// Allow HTTP (non-TLS) connections. Default is false.
    /// </summary>
    public bool? AllowHttp { get; set; }

    /// <summary>
    /// Use virtual hosted style requests. Default is false.
    /// </summary>
    public bool? VirtualHostedStyleRequest { get; set; }

    /// <summary>
    /// Skip signing requests, for anonymous access to public buckets. Default is false.
    /// </summary>
    public bool? SkipSignature { get; set; }
}
