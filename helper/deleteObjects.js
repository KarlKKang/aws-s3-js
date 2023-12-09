import { DeleteObjectsCommand } from '@aws-sdk/client-s3';

export async function deleteObjects(client, bucket, keys) {
    const response = await client.send(
        new DeleteObjectsCommand({
            Bucket: bucket,
            Delete: {
                Objects: keys.map((key) => ({ Key: xmlEscape(key) })),
            },
        })
    );
    return {
        deletedKeys: response.Deleted === undefined ? [] : response.Deleted.map((deleted) => deleted.Key),
        failedKeys: response.Errors === undefined ? [] : response.Errors.map((error) => error.Key),
    }
}

function xmlEscape(str) {
    return str.replace(/[<>&'"\r\n]/g, function (c) {
        switch (c) {
            case '<': return '&lt;';
            case '>': return '&gt;';
            case '&': return '&amp;';
            case '\'': return '&apos;';
            case '"': return '&quot;';
            case '\r': return '&#x0D;';
            case '\n': return '&#x0A;';
        }
    });
}