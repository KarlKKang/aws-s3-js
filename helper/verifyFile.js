import { open, read, close } from './fs.js';
import { createHash } from 'crypto';

const CHUNK_SIZE = 16 * 1024 * 1024;

export async function verifyFile(objectChecksum, localFilePath) {
    const fd = await open(localFilePath);
    const buffer = Buffer.alloc(CHUNK_SIZE);
    let totalBytesRead = 0;

    if (objectChecksum.parts) {
        const partChecksums = [];
        for (const part of objectChecksum.parts) {
            let remoteChecksum;
            try {
                remoteChecksum = parseRemoteChecksum(part);
            } catch (e) {
                close(fd);
                throw e;
            }
            const hash = createHash(remoteChecksum[1]);
            let partBytesRead = 0;
            let bytesRead = await read(fd, buffer, Math.min(CHUNK_SIZE, part.size));
            while (bytesRead > 0) {
                hash.update(buffer.subarray(0, bytesRead));
                partBytesRead += bytesRead;
                bytesRead = await read(fd, buffer, Math.min(CHUNK_SIZE, part.size - partBytesRead));
            }
            totalBytesRead += partBytesRead;
            const localResult = hash.digest();
            if (remoteChecksum[0] !== localResult.toString('base64')) {
                close(fd);
                return false;
            }
            partChecksums.push(localResult);
        }
        close(fd);
        if (totalBytesRead !== objectChecksum.size) {
            return false;
        }
        const remoteCombinedChecksum = parseRemoteChecksum(objectChecksum.checksum);
        const hash = createHash(remoteCombinedChecksum[1]);
        for (const partChecksum of partChecksums) {
            hash.update(partChecksum);
        }
        if (remoteCombinedChecksum[0] !== hash.digest().toString('base64')) {
            return false;
        }
        return true;
    }

    let remoteChecksum;
    try {
        remoteChecksum = parseRemoteChecksum(objectChecksum.checksum);
    } catch (e) {
        close(fd);
        throw e;
    }
    const hash = createHash(remoteChecksum[1]);
    let bytesRead = await read(fd, buffer, CHUNK_SIZE);
    while (bytesRead > 0) {
        hash.update(buffer.subarray(0, bytesRead));
        totalBytesRead += bytesRead;
        bytesRead = await read(fd, buffer, CHUNK_SIZE);
    }
    close(fd);
    if (totalBytesRead !== objectChecksum.size) {
        return false;
    }
    if (remoteChecksum[0] === hash.digest().toString('base64')) {
        return true;
    }
    return false;
}

function parseRemoteChecksum(obj) {
    if (obj.ChecksumSHA256) {
        return [obj.ChecksumSHA256, 'sha256'];
    }
    if (obj.ChecksumSHA1) {
        return [obj.ChecksumSHA1, 'sha1'];
    }
    if (obj.ChecksumCRC32C) {
        throw new Error('crc32c not currently supported.');
    }
    if (obj.ChecksumCRC32) {
        throw new Error('crc32 not currently supported.');
    }
    throw new Error('Unknown checksum type.');
}