---
sidebar_position: 5
title: Object Stores
---

# Object Stores

Object stores let you query remote data via URL schemes. The pattern is: **register the store**, then **register tables using URLs** instead of local paths.

## URL Schemes

| Scheme           | Store                      |
|------------------|----------------------------|
| `file:///`       | Local filesystem           |
| `s3://bucket`    | Amazon S3 (and compatible) |
| `az://container` | Azure Blob Storage         |
| `gs://bucket`    | Google Cloud Storage       |
| `memory://`      | In-memory (ephemeral)      |

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

| Property                    | Type      | Required | Description                                |
|-----------------------------|-----------|----------|--------------------------------------------|
| `BucketName`                | `string`  | Yes      | S3 bucket name                             |
| `Region`                    | `string?` | No       | AWS region (e.g., `us-east-1`)             |
| `AccessKeyId`               | `string?` | No       | AWS access key ID                          |
| `SecretAccessKey`           | `string?` | No       | AWS secret access key                      |
| `Endpoint`                  | `string?` | No       | Custom endpoint for S3-compatible services |
| `Token`                     | `string?` | No       | Session token for temporary credentials    |
| `AllowHttp`                 | `bool?`   | No       | Allow HTTP (non-TLS) connections           |
| `VirtualHostedStyleRequest` | `bool?`   | No       | Use virtual hosted style requests          |
| `SkipSignature`             | `bool?`   | No       | Skip request signing (for public buckets)  |

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
    SasKey = "...",
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

| Property        | Type      | Required | Description                     |
|-----------------|-----------|----------|---------------------------------|
| `ContainerName` | `string`  | Yes      | Azure container name            |
| `AccountName`   | `string?` | No       | Storage account name            |
| `AccessKey`     | `string?` | No       | Storage access key              |
| `BearerToken`   | `string?` | No       | Bearer token                    |
| `ClientId`      | `string?` | No       | Service principal client ID     |
| `ClientSecret`  | `string?` | No       | Service principal client secret |
| `TenantId`      | `string?` | No       | Service principal tenant ID     |
| `SasKey`        | `string?` | No       | Shared Access Signature key     |
| `Endpoint`      | `string?` | No       | Custom endpoint URL             |
| `UseEmulator`   | `bool?`   | No       | Use Azurite emulator            |
| `AllowHttp`     | `bool?`   | No       | Allow HTTP connections          |
| `SkipSignature` | `bool?`   | No       | Skip request signing            |

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

| Property              | Type      | Required | Description                           |
|-----------------------|-----------|----------|---------------------------------------|
| `BucketName`          | `string`  | Yes      | GCS bucket name                       |
| `ProjectId`           | `string?` | No       | GCP project ID                        |
| `CredentialsPath`     | `string?` | No       | Path to service account JSON key file |
| `Credentials`         | `string?` | No       | Service account JSON key as string    |
| `ServiceAccountEmail` | `string?` | No       | Service account email                 |
| `CustomEndpoint`      | `string?` | No       | Custom endpoint URL                   |
| `AllowHttp`           | `bool?`   | No       | Allow HTTP connections                |
| `SkipSignature`       | `bool?`   | No       | Skip request signing                  |

## In-Memory Object Store

The in-memory object store keeps data entirely in memory. It is useful for tests, short-lived pipelines, or any scenario where you want to load data from an external source (database, HTTP API, message queue, etc.) and query it with SQL without writing temporary files.

### Basic Usage

```csharp
using var runtime = DataFusionRuntime.Create();
using var context = runtime.CreateSessionContext();

// 1. Create the store
using var store = runtime.CreateInMemoryStore();

// 2. Put data into the store
var csvBytes = await File.ReadAllBytesAsync("data/customers.csv");
await store.PutAsync("customers.csv", csvBytes);

// 3. Register the store with a URL scheme
context.RegisterInMemoryObjectStore("memory://", store);

// 4. Register a table using the memory:// URL and query it
await context.RegisterCsvAsync("customers", "memory:///customers.csv");

using var df = await context.SqlAsync("SELECT * FROM customers");
await df.ShowAsync();
```

Any file format works — CSV, JSON, or Parquet:

```csharp
await store.PutAsync("events.parquet", parquetBytes);
context.RegisterInMemoryObjectStore("memory://", store);
await context.RegisterParquetAsync("events", "memory:///events.parquet");
```

### Deleting Data

Remove an object from the store with `DeleteAsync`. Subsequent queries against the table will return no rows (the table registration remains, but the underlying data is gone):

```csharp
await store.DeleteAsync("customers.csv");
```

### Zero-Copy Put

`PutAsStaticAsync` avoids copying data into native memory. The caller **must** pin the memory and keep it alive for the entire lifetime of the store:

```csharp
var csvBytes = await File.ReadAllBytesAsync("data/customers.csv");
using var pin = csvBytes.AsMemory().Pin();

await store.PutAsStaticAsync("customers.csv", pin, csvBytes.Length);
```

> **Warning:** If the pinned memory is released or modified while the store is still in use, behaviour is undefined.

### Multiple Stores

You can register several in-memory stores on the same session by giving each one a distinct URL scheme:

```csharp
using var storeA = runtime.CreateInMemoryStore();
using var storeB = runtime.CreateInMemoryStore();

await storeA.PutAsync("customers.csv", customersCsv);
await storeB.PutAsync("orders.csv", ordersCsv);

context.RegisterInMemoryObjectStore("mem-a://", storeA);
context.RegisterInMemoryObjectStore("mem-b://", storeB);

await context.RegisterCsvAsync("customers", "mem-a:///customers.csv");
await context.RegisterCsvAsync("orders", "mem-b:///orders.csv");

using var df = await context.SqlAsync(
    "SELECT c.customer_name, o.order_id FROM customers c JOIN orders o ON c.customer_id = o.customer_id");
```

### Sharing a Store Across Sessions

A single store can be registered in multiple session contexts. Each session maintains its own table catalog, but they share the underlying data:

```csharp
context.RegisterInMemoryObjectStore("memory://", store);
contextB.RegisterInMemoryObjectStore("memory://", store);
```

### InMemoryObjectStore API

| Method                                                           | Description                                                     |
|------------------------------------------------------------------|-----------------------------------------------------------------|
| `PutAsync(string path, Memory<byte> data)`                       | Copies `data` into the store at `path`.                         |
| `PutAsStaticAsync(string path, MemoryHandle handle, int length)` | Stores data without copying. The pinned memory must stay alive. |
| `DeleteAsync(string path)`                                       | Removes the object at `path`.                                   |
| `Dispose()`                                                      | Releases all native resources held by the store.                |

## Deregistering Object Stores

Remove a previously registered object store by its URL scheme. After deregistration, tables that reference the store's URLs can no longer access data.

```csharp
context.DeregisterObjectStore("s3://my-bucket");
```