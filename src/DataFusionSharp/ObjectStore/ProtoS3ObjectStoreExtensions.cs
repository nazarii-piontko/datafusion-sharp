namespace DataFusionSharp.ObjectStore;

internal static class ProtoS3ObjectStoreExtensions
{
    internal static Proto.S3ObjectStoreOptions ToProto(this S3ObjectStoreOptions options)
    {
        var proto = new Proto.S3ObjectStoreOptions { BucketName = options.BucketName };

        if (options.Region is not null)
            proto.Region = options.Region;

        if (options.AccessKeyId is not null)
            proto.AccessKeyId = options.AccessKeyId;

        if (options.SecretAccessKey is not null)
            proto.SecretAccessKey = options.SecretAccessKey;

        if (options.Endpoint is not null)
            proto.Endpoint = options.Endpoint;

        if (options.Token is not null)
            proto.Token = options.Token;

        if (options.AllowHttp is not null)
            proto.AllowHttp = options.AllowHttp.Value;

        if (options.VirtualHostedStyleRequest is not null)
            proto.VirtualHostedStyleRequest = options.VirtualHostedStyleRequest.Value;

        if (options.SkipSignature is not null)
            proto.SkipSignature = options.SkipSignature.Value;

        return proto;
    }
}
