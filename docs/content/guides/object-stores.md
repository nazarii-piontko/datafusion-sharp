---
sidebar_position: 5
title: Object Stores
---

# Object Stores

Object stores let you query remote data via URL schemes. The pattern is: **register the store**, then **register tables using URLs** instead of local paths.

## URL Schemes

| Scheme | Store |
|--------|-------|
| `file:///` | Local filesystem |
| `s3://bucket` | Amazon S3 (and compatible) |
| `az://container` | Azure Blob Storage |
| `gs://bucket` | Google Cloud Storage |

## Local Filesystem

```csharp
context.RegisterLocalFileSystem("file:///");

await context.RegisterCsvAsync("orders", "file:///data/orders.csv");
using var df = await context.SqlAsync("SELECT * FROM orders");
```

## Amazon S3

```csharp
// Public bucket (anonymous access)
context.RegisterS3ObjectStore("s3://arrow-datasets", new S3ObjectStoreOptions
{
    BucketName = "arrow-datasets",
    Region = "us-east-2",
    SkipSignature = true,
});

await context.RegisterParquetAsync("data", "s3://arrow-datasets/data.parquet");
```

### S3 with Credentials

```csharp
context.RegisterS3ObjectStore("s3://my-bucket", new S3ObjectStoreOptions
{
    BucketName = "my-bucket",
    Region = "us-west-2",
    AccessKeyId = "AKIA...",
    SecretAccessKey = "secret",
});
```

### S3-Compatible (MinIO)

```csharp
context.RegisterS3ObjectStore("s3://my-bucket", new S3ObjectStoreOptions
{
    BucketName = "my-bucket",
    Endpoint = "http://localhost:9000",
    AccessKeyId = "minioadmin",
    SecretAccessKey = "minioadmin",
    AllowHttp = true,
});
```

### S3ObjectStoreOptions

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `BucketName` | `string` | Yes | S3 bucket name |
| `Region` | `string?` | No | AWS region (e.g., `us-east-1`) |
| `AccessKeyId` | `string?` | No | AWS access key ID |
| `SecretAccessKey` | `string?` | No | AWS secret access key |
| `Endpoint` | `string?` | No | Custom endpoint for S3-compatible services |
| `Token` | `string?` | No | Session token for temporary credentials |
| `AllowHttp` | `bool?` | No | Allow HTTP (non-TLS) connections |
| `VirtualHostedStyleRequest` | `bool?` | No | Use virtual hosted style requests |
| `SkipSignature` | `bool?` | No | Skip request signing (for public buckets) |

## Azure Blob Storage

```csharp
context.RegisterAzureBlobStorage("az://my-container", new AzureBlobStorageOptions
{
    ContainerName = "my-container",
    AccountName = "myaccount",
    AccessKey = "base64key...",
});

await context.RegisterParquetAsync("data", "az://my-container/path/data.parquet");
```

### Azure with SAS Token

```csharp
context.RegisterAzureBlobStorage("az://my-container", new AzureBlobStorageOptions
{
    ContainerName = "my-container",
    AccountName = "myaccount",
    SasKey = "sv=2021-06-08&ss=b&srt=sco...",
});
```

### Azure with Service Principal

```csharp
context.RegisterAzureBlobStorage("az://my-container", new AzureBlobStorageOptions
{
    ContainerName = "my-container",
    ClientId = "client-id",
    ClientSecret = "client-secret",
    TenantId = "tenant-id",
});
```

### Azurite Emulator

```csharp
context.RegisterAzureBlobStorage("az://my-container", new AzureBlobStorageOptions
{
    ContainerName = "my-container",
    UseEmulator = true,
    AllowHttp = true,
});
```

### AzureBlobStorageOptions

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `ContainerName` | `string` | Yes | Azure container name |
| `AccountName` | `string?` | No | Storage account name |
| `AccessKey` | `string?` | No | Storage access key |
| `BearerToken` | `string?` | No | Bearer token |
| `ClientId` | `string?` | No | Service principal client ID |
| `ClientSecret` | `string?` | No | Service principal client secret |
| `TenantId` | `string?` | No | Service principal tenant ID |
| `SasKey` | `string?` | No | Shared Access Signature key |
| `Endpoint` | `string?` | No | Custom endpoint URL |
| `UseEmulator` | `bool?` | No | Use Azurite emulator |
| `AllowHttp` | `bool?` | No | Allow HTTP connections |
| `SkipSignature` | `bool?` | No | Skip request signing |

## Google Cloud Storage

```csharp
// With service account credentials file
context.RegisterGoogleCloudStorage("gs://my-bucket", new GoogleCloudStorageOptions
{
    BucketName = "my-bucket",
    CredentialsPath = "/path/to/service-account.json",
});

await context.RegisterParquetAsync("data", "gs://my-bucket/data.parquet");
```

### Anonymous Access

```csharp
context.RegisterGoogleCloudStorage("gs://public-bucket", new GoogleCloudStorageOptions
{
    BucketName = "public-bucket",
    SkipSignature = true,
});
```

### GoogleCloudStorageOptions

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `BucketName` | `string` | Yes | GCS bucket name |
| `ProjectId` | `string?` | No | GCP project ID |
| `CredentialsPath` | `string?` | No | Path to service account JSON key file |
| `Credentials` | `string?` | No | Service account JSON key as string |
| `ServiceAccountEmail` | `string?` | No | Service account email |
| `CustomEndpoint` | `string?` | No | Custom endpoint URL |
| `AllowHttp` | `bool?` | No | Allow HTTP connections |
| `SkipSignature` | `bool?` | No | Skip request signing |

## Deregistering Object Stores

```csharp
context.DeregisterObjectStore("s3://my-bucket");
```
