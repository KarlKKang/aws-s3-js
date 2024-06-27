import { DeleteObjectsCommand } from '@aws-sdk/client-s3';

export async function deleteObjects(client, bucket, keys) {
    const response = await client.send(
        new DeleteObjectsCommand({
            Bucket: bucket,
            Delete: {
                Objects: keys.map((key) => ({ Key: key })),
            },
        })
    );
    return {
        deletedKeys: response.Deleted === undefined ? [] : response.Deleted.map((deleted) => deleted.Key),
        failedKeys: response.Errors === undefined ? [] : response.Errors.map((error) => error.Key),
    }
}