import { GetObjectAttributesCommand } from '@aws-sdk/client-s3';

export async function getObjectChecksum(client, bucket, key) {
    let response = await client.send(
        new GetObjectAttributesCommand({
            Bucket: bucket,
            Key: key,
            ObjectAttributes: ['ObjectParts', 'Checksum', 'ObjectSize'],
        })
    );
    return {
        size: response.ObjectSize,
        checksum: {
            ...parseChecksumObject(response.Checksum),
        },
        parts: await getPartChecksums(client, bucket, key, response.ObjectParts),
    };
}

async function getPartChecksums(client, bucket, key, initialObjectParts) {
    if (!initialObjectParts) {
        return undefined;
    }
    const partChecksums = [];
    for (const part of initialObjectParts.Parts) {
        partChecksums.push({
            partNumber: part.PartNumber,
            size: part.Size,
            ...parseChecksumObject(part)
        });
    }
    let objectParts = initialObjectParts;
    while (objectParts.IsTruncated) {
        objectParts = await client.send(
            new GetObjectAttributesCommand({
                Bucket: bucket,
                Key: key,
                ObjectAttributes: ['ObjectParts'],
                PartNumberMarker: objectParts.NextPartNumberMarker,
            })
        );
        for (const part of objectParts.Parts) {
            partChecksums.push({
                partNumber: part.PartNumber,
                size: part.Size,
                ...parseChecksumObject(part)
            });
        }
    }
    return partChecksums;
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