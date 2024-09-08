import { CreateMultipartUploadCommand, UploadPartCommand, CompleteMultipartUploadCommand, AbortMultipartUploadCommand } from '@aws-sdk/client-s3';

export async function createMultipartUpload(client, bucket, key, mime, cacheControl, metadata) {
    const command = new CreateMultipartUploadCommand({
        Bucket: bucket,
        Key: key,
        ContentType: mime,
        ChecksumAlgorithm: 'SHA256',
        CacheControl: cacheControl,
        Metadata: metadata,
    });
    const response = await client.send(command);
    return {
        bucket: bucket,
        key: key,
        uploadId: response.UploadId
    };
}

export async function uploadPart(client, multipartUploadJob, partNumber, buffer, sha256, md5) {
    const command = new UploadPartCommand({
        Bucket: multipartUploadJob.bucket,
        Key: multipartUploadJob.key,
        UploadId: multipartUploadJob.uploadId,
        PartNumber: partNumber,
        Body: buffer,
        ContentLength: buffer.length,
        ChecksumAlgorithm: 'SHA256',
        ChecksumSHA256: sha256,
        ContentMD5: md5,
    });
    const response = await client.send(command);
    if (multipartUploadJob.parts === undefined) {
        multipartUploadJob.parts = [];
    }
    multipartUploadJob.parts[partNumber - 1] = {
        ETag: response.ETag,
        PartNumber: partNumber,
        ChecksumSHA256: sha256,
    };
}

export async function completeMultipartUpload(client, multipartUploadJob, sha256) {
    const command = new CompleteMultipartUploadCommand({
        Bucket: multipartUploadJob.bucket,
        Key: multipartUploadJob.key,
        UploadId: multipartUploadJob.uploadId,
        ChecksumSHA256: sha256,
        MultipartUpload: {
            Parts: multipartUploadJob.parts,
        },
    });
    await client.send(command);
}

export async function abortMultipartUpload(client, multipartUploadJob) {
    const command = new AbortMultipartUploadCommand({
        Bucket: multipartUploadJob.bucket,
        Key: multipartUploadJob.key,
        UploadId: multipartUploadJob.uploadId,
    });
    await client.send(command);
}