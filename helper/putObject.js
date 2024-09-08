import { PutObjectCommand } from '@aws-sdk/client-s3';

export async function putObject(client, bucket, key, buffer, mime, sha256, md5, cacheControl, metadata) {
    const command = new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: buffer,
        ContentLength: buffer.length,
        ChecksumAlgorithm: 'SHA256',
        ChecksumSHA256: sha256,
        ContentMD5: md5,
        ContentType: mime,
        CacheControl: cacheControl,
        Metadata: metadata,
    });
    await client.send(command);
}