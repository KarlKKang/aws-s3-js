import { ListObjectsV2Command } from '@aws-sdk/client-s3';

export async function listObjectsV2(client, bucket, prefix) {
    const results = {};

    let response = await client.send(
        new ListObjectsV2Command({
            Bucket: bucket,
            Prefix: prefix,
        })
    );
    if (!response.Contents) return results;
    for (const content of response.Contents) {
        results[content.Key] = {
            size: content.Size,
            mtime: content.LastModified,
        };
    }

    while (response.IsTruncated) {
        const token = response.NextContinuationToken;
        response = await client.send(
            new ListObjectsV2Command({
                Bucket: bucket,
                Prefix: prefix,
                ContinuationToken: token,
            })
        );
        if (!response.Contents) break;
        for (const content of response.Contents) {
            results[content.Key] = {
                size: content.Size,
                mtime: content.LastModified,
            };
        }
    }

    return results;
}