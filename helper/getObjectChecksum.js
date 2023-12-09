import { HeadObjectCommand } from '@aws-sdk/client-s3';

export async function getObjectChecksum(client, bucket, key) {
    // This function does not use the faster `GetObjectAttributesCommand` API because currently there is a bug that causes internal server errors when the object key contains some special characters.
    let response = await client.send(
        new HeadObjectCommand({
            Bucket: bucket,
            Key: key,
            ChecksumMode: 'ENABLED',
            PartNumber: 1,
        })
    );
    let parts = undefined;
    const partsCount = response.PartsCount;
    if (partsCount) {
        parts = [
            {
                partNumber: 1,
                size: response.ContentLength,
                ...parseChecksumObject(response),
            }
        ];
        const partsRequests = [];
        for (let i = 2; i <= partsCount; i++) {
            partsRequests.push(
                client.send(
                    new HeadObjectCommand({
                        Bucket: bucket,
                        Key: key,
                        ChecksumMode: 'ENABLED',
                        PartNumber: i,
                    })
                )
            );
        }
        response = await client.send(
            new HeadObjectCommand({
                Bucket: bucket,
                Key: key,
                ChecksumMode: 'ENABLED',
            })
        );
        const partsResponses = await Promise.all(partsRequests);
        for (const partsResponse of partsResponses) {
            parts.push({
                partNumber: parts.length + 1,
                size: partsResponse.ContentLength,
                ...parseChecksumObject(partsResponse),
            });
        }
    }
    const checksumObject = parseChecksumObject(response);
    for (const key in checksumObject) {
        const endIndex = checksumObject[key].indexOf('-');
        if (endIndex >= 0) {
            checksumObject[key] = checksumObject[key].substring(0, endIndex);
        }
    }
    return {
        size: response.ContentLength,
        checksum: {
            ...checksumObject,
        },
        parts: parts,
    };
}

function parseChecksumObject(obj) {
    if (obj.ChecksumCRC32) {
        return { ChecksumCRC32: obj.ChecksumCRC32 };
    }
    if (obj.ChecksumCRC32C) {
        return { ChecksumCRC32C: obj.ChecksumCRC32C };
    }
    if (obj.ChecksumSHA1) {
        return { ChecksumSHA1: obj.ChecksumSHA1 };
    }
    if (obj.ChecksumSHA256) {
        return { ChecksumSHA256: obj.ChecksumSHA256 };
    }
    return {};
}