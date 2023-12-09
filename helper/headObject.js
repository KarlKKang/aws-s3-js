import { HeadObjectCommand } from '@aws-sdk/client-s3';

export async function headObject(client, bucket, key) {
    const response = await client.send(
        new HeadObjectCommand({
            Bucket: bucket,
            Key: key,
        })
    );
    return {
        size: response.ContentLength,
        mtime: response.LastModified,
    };
}