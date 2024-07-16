import { S3Client } from '@aws-sdk/client-s3';
import { close, getFileAttributes, open, read, scanDir } from './helper/fs.js';
import { createQueue, enqueue, dequeue } from './helper/queue.js';
import path from 'path';
import { headObject } from './helper/headObject.js';
import { createHash } from 'crypto';
import { MIME } from './helper/mime.js';
import { putObject } from './helper/putObject.js';
import { createMultipartUpload, uploadPart, abortMultipartUpload, completeMultipartUpload } from './helper/multipartUpload.js';
import { listObjectsV2 } from './helper/listObjectsV2.js';
import { deleteObjects } from './helper/deleteObjects.js';

function printHelp() {
    console.error('Usage:');
    console.error(process.argv0 + ' ' + process.argv[1] + ' <BUCKET> <REMOTE_PATH> <LOCAL_PATH> [OPTIONS]');
    console.error();
    console.error('Options:');
    console.error('--region <REGION>');
    console.error('    Use the given region instead of the default region.');
    console.error('--exclude <REGEXP>');
    console.error('    Exclude files matching the given regular expression. This option is ignored when uploading a single file.');
    console.error('--include <REGEXP>');
    console.error('    Include files matching the given regular expression. This option is ignored when uploading a single file.');
    console.error('--mime <EXT> <TYPE>');
    console.error('    Set the MIME type for files with the given extension. Multiple --mime options can be specified. <EXT> is a regular expression and <TYPE> is a MIME type.');
    console.error('--threads <COUNT>');
    console.error('    Upload the given number of files concurrently. The default is 8. This value is ignored when uploading a single file. If you are running out of memory, try reducing this value.');
    console.error('--multipart-threads <COUNT>');
    console.error('    Use the given number of concurrent network requests for multipart uploads. The default is 16. If you are running out of memory, try reducing this value.');
    console.error('--delete');
    console.error('    Delete remote files that do not exist locally. This option is ignored when uploading a single file.');
    console.error('--no-overwrite');
    console.error('    Do not overwrite existing files.');
    console.error();
    console.error('Do not include enclosing slashes of the regular expressions. All regular expressions are case-insensitive and can match unicode characters. The regular expressions are matched against the relative path of the file, which is the path relative to <LOCAL_PATH> if it is a directory. Otherwise, the absolute path of the file is used.');
}

const bucket = process.argv[2];
let remotePath = process.argv[3];
let localPath = process.argv[4];

if (!bucket || !remotePath || !localPath) {
    console.error('Error: Missing required argument');
    console.error();
    printHelp();
    process.exit(1);
}

let region = undefined;
let exclude = undefined;
let include = undefined;
let mime = [];
let threads = 8;
let multipartThreads = 16;
let deleteRemote = false;
let overwrite = true;
for (let i = 5; i < process.argv.length; i++) {
    if (process.argv[i] === '--region') {
        region = process.argv[++i];
        if (!region) {
            console.error('Error: Expected argument after --region');
            console.error();
            printHelp();
            process.exit(1);
        }
    } else if (process.argv[i] === '--exclude') {
        exclude = process.argv[++i];
        if (!exclude) {
            console.error('Error: Expected argument after --exclude');
            console.error();
            printHelp();
            process.exit(1);
        }
        exclude = new RegExp(exclude, 'iu');
    } else if (process.argv[i] === '--include') {
        include = process.argv[++i];
        if (!include) {
            console.error('Error: Expected argument after --include');
            console.error();
            printHelp();
            process.exit(1);
        }
        include = new RegExp(include, 'iu');
    } else if (process.argv[i] === '--mime') {
        const ext = process.argv[++i];
        const type = process.argv[++i];
        if (!ext || !type) {
            console.error('Error: Expected argument after --mime');
            console.error();
            printHelp();
            process.exit(1);
        }
        mime.push([new RegExp(ext), type]);
    } else if (process.argv[i] === '--threads') {
        threads = parseInt(process.argv[++i]);
        if (threads < 1 || threads > 256 || isNaN(threads)) {
            console.error('Error: Expected integer between 1 and 256 after --threads');
            console.error();
            printHelp();
            process.exit(1);
        }
    } else if (process.argv[i] === '--multipart-threads') {
        multipartThreads = parseInt(process.argv[++i]);
        if (multipartThreads < 1 || multipartThreads > 256 || isNaN(multipartThreads)) {
            console.error('Error: Expected integer between 1 and 256 after --multipart-threads');
            console.error();
            printHelp();
            process.exit(1);
        }
    } else if (process.argv[i] === '--delete') {
        deleteRemote = true;
    } else if (process.argv[i] === '--no-overwrite') {
        overwrite = false;
    } else {
        console.error('Error: Unknown option: ' + process.argv[i]);
        console.error();
        printHelp();
        process.exit(1);
    }
}

localPath = path.resolve(localPath);
if (remotePath.startsWith('/')) {
    remotePath = remotePath.substring(1);
}

const MIN_PART_SIZE = 64 * 1024 * 1024;
const MULTIPART_SIZE_THREASHOLD = 512 * 1024 * 1024;
let runningMultipartUploads = 0;
const totalMultipartThreads = multipartThreads;

const client = new S3Client({
    region: region,
});

const fileAttributes = await getFileAttributes(localPath);
if (fileAttributes.isDirectory) {
    uploadDir();
} else if (fileAttributes.isFile) {
    const uploadResult = await uploadFile(remotePath, localPath, getMime(localPath));
    if (uploadResult === 0) {
        console.log('Successfully uploaded ' + localPath + ' to ' + remotePath);
    } else if (uploadResult === 1) {
        console.log('Skipped ' + localPath + ' because it is already up to date.');
    } else {
        console.error('Failed to upload ' + localPath + ' to ' + remotePath + ': file already exists and overwrite is disabled.');
        process.exit(1);
    }
} else {
    console.error('Error: Path is not a file or directory: ' + localPath);
    process.exit(1);
}

async function uploadDir() {
    const localRoot = localPath;
    if (remotePath !== '' && !remotePath.endsWith('/')) {
        remotePath += '/';
    }
    const remoteRoot = remotePath;
    const [remoteFiles, localPaths] = await Promise.all([
        listObjectsV2(client, bucket, remoteRoot),
        scanDir(localPath),
    ]);

    const totalLocalCount = localPaths.length;
    const totalRemoteCount = Object.keys(remoteFiles).length;
    let localExcludeCount = 0;
    const pendingLocalPaths = createQueue();
    for (const localPath of localPaths) {
        const relativePath = localToRelativePath(localPath, localRoot);
        if (exclude && exclude.test(relativePath)) {
            if (!include || !include.test(relativePath)) {
                localExcludeCount++;
                continue;
            }
        }
        enqueue(pendingLocalPaths, (async () => {
            const localFile = await getFileAttributes(localPath);
            const remotePath = remoteRoot + relativePath;
            const remoteFile = remoteFiles[remotePath];
            if (remoteFile) {
                delete remoteFiles[remotePath];
            }
            if (remoteFile && remoteFile.size === localFile.size && remoteFile.mtime.getTime() >= localFile.mtime.getTime()) {
                return false;
            }
            return [remotePath, localPath, relativePath, localFile.size];
        })());
    }

    let skippedCount = 0;
    let totalSize = 0;
    const pendingLocalPathsCount = pendingLocalPaths.length;
    for (let i = 0; i < pendingLocalPathsCount; i++) {
        const result = await dequeue(pendingLocalPaths);
        if (result === false) {
            skippedCount++;
            continue;
        }
        totalSize += result[3];
        enqueue(pendingLocalPaths, result);
    }

    const averageSize = totalSize / pendingLocalPaths.length;
    let largeFilePaths = createQueue();
    let smallFilePaths = createQueue();
    let pendingLocalPath = dequeue(pendingLocalPaths);
    while (pendingLocalPath !== null) {
        if (pendingLocalPath[3] > averageSize) {
            enqueue(largeFilePaths, pendingLocalPath);
        } else {
            enqueue(smallFilePaths, pendingLocalPath);
        }
        pendingLocalPath = dequeue(pendingLocalPaths);
    }

    const workers = [];
    let successCount = 0;
    let failedCount = 0;
    for (let i = 0; i < threads; i++) {
        workers.push((async () => {
            let primaryQueue = largeFilePaths;
            let secondaryQueue = smallFilePaths;
            if (i !== 0) {
                primaryQueue = smallFilePaths;
                secondaryQueue = largeFilePaths;
            }
            let localPendingPath = dequeue(primaryQueue) ?? dequeue(secondaryQueue);
            while (localPendingPath !== null) {
                try {
                    const [remotePath, localPath, relativePath] = localPendingPath;
                    const uploadResult = await uploadFile(remotePath, localPath, getMime(relativePath));
                    if (uploadResult === 0) {
                        console.log('Successfully uploaded ' + localPath + ' to ' + remotePath);
                        successCount++;
                    } else if (uploadResult === 1) {
                        skippedCount++;
                    } else {
                        console.error('Failed to upload ' + localPath + ' to ' + remotePath + ': file already exists and overwrite is disabled.');
                        failedCount++;
                    }
                } catch (e) {
                    console.error('Failed to upload ' + localPath + ' to ' + remotePath);
                    failedCount++;
                }
                localPendingPath = dequeue(primaryQueue) ?? dequeue(secondaryQueue);
            }
        })());
    }

    let deletedCount = 0;
    let failedDeleteCount = 0;
    let remoteExcludeCount = 0;
    if (deleteRemote) {
        const deleteKeys = [];
        for (const remotePath in remoteFiles) {
            const relativePath = remotePath.substring(remoteRoot.length);
            if (exclude && exclude.test(relativePath)) {
                if (!include || !include.test(relativePath)) {
                    remoteExcludeCount++;
                    continue;
                }
            }
            deleteKeys.push(remotePath);
        }
        const requestCount = Math.ceil(deleteKeys.length / 1000);
        const deleteWorkers = [];
        for (let i = 0; i < requestCount; i++) {
            deleteWorkers.push((async () => {
                const deleteResult = await requestWrapper(async () => {
                    return await deleteObjects(client, bucket, deleteKeys.slice(i * 1000, (i + 1) * 1000));
                });
                for (const deletedKey of deleteResult.deletedKeys) {
                    console.log('Deleted ' + deletedKey);
                }
                for (const failedKey of deleteResult.failedKeys) {
                    console.error('Failed to delete ' + failedKey);
                }
                deletedCount += deleteResult.deletedKeys.length;
                failedDeleteCount += deleteResult.failedKeys.length;
            })());
        }
        await Promise.all(deleteWorkers);
    }

    await Promise.all(workers);

    console.log();
    console.log('Total local files: ' + totalLocalCount);
    console.log('Total remote files: ' + totalRemoteCount);
    console.log('Successfully uploaded ' + successCount + ' files.');
    console.log('Failed to upload ' + failedCount + ' files.');
    console.log('Deleted ' + deletedCount + ' files.');
    console.log('Failed to delete ' + failedDeleteCount + ' files.');
    console.log('Skipped ' + skippedCount + ' files (already up to date).');
    console.log('Excluded ' + localExcludeCount + ' local files from upload.');
    console.log('Excluded ' + remoteExcludeCount + ' remote files from deletion.');
    if (failedCount > 0 || failedDeleteCount > 0) {
        process.exit(1);
    }
}

async function uploadFile(remotePath, localPath, mimeType) {
    // Do a final check before uploading.
    let localFile = getFileAttributes(localPath);
    let remoteFile = undefined;
    try {
        remoteFile = await headObject(client, bucket, remotePath);
    } catch (e) {
        if (!e['$metadata'] || e['$metadata'].httpStatusCode !== 404) {
            throw e;
        }
    }
    localFile = await localFile;
    if (remoteFile && remoteFile.size === localFile.size && remoteFile.mtime.getTime() >= localFile.mtime.getTime()) {
        return 1;
    }
    if (remoteFile && !overwrite) {
        return 2;
    }
    if (localFile.size <= MULTIPART_SIZE_THREASHOLD) {
        await singlePartUpload(localPath, remotePath, localFile.size, mimeType);
    } else if (localFile.size <= 5 * 1024 * 1024 * 1024 * 1024) {
        await multipartUpload(localPath, remotePath, localFile.size, mimeType);
    } else {
        throw new Error('File is too large: ' + localPath);
    }
    return 0;
}

async function singlePartUpload(localPath, remotePath, size, mime) {
    let buffer = Buffer.alloc(size + 1);
    const fd = await open(localPath);
    const bytesRead = await read(fd, buffer, size + 1);
    close(fd);
    if (bytesRead !== size) {
        throw new Error('Failed to read file: ' + localPath);
    }
    buffer = buffer.subarray(0, size);
    const sha256 = createHash('sha256').update(buffer).digest().toString('base64');
    const md5 = createHash('md5').update(buffer).digest().toString('base64');
    await requestWrapper(async () => {
        await putObject(client, bucket, remotePath, buffer, mime, sha256, md5);
    });
}

async function multipartUpload(localPath, remotePath, size, mime) {
    const partSize = Math.max(MIN_PART_SIZE, Math.ceil(size / 10000));
    const multipartUploadJob = await requestWrapper(async () => {
        return await createMultipartUpload(client, bucket, remotePath, mime);
    });
    const fd = await open(localPath);
    const chunkChecksums = [];
    let totalBytesRead = 0;
    let partNumber = 1;
    let chunks = createQueue();
    let runningUploadWorkers = 0;
    let chunkWorkerRunning = true;
    let returnPromiseResolve;
    let returnPromiseReject;
    const returnPromise = new Promise((resolve, reject) => {
        returnPromiseResolve = resolve;
        returnPromiseReject = reject;
    });
    const spawnUploadWorker = async () => {
        const chunk = dequeue(chunks);
        if (!chunkWorkerRunning) {
            chunkWorkerRunning = true;
            spawnChunkWorker();
        }
        try {
            await requestWrapper(async () => {
                await uploadPart(client, multipartUploadJob, ...chunk);
            });
        } catch (e) {
            returnPromiseReject(e);
            return;
        }
        if (chunks.length > 0) {
            spawnUploadWorker();
        } else {
            runningUploadWorkers--;
            if (runningUploadWorkers === 0 && !chunkWorkerRunning) {
                partsCompletedCallback();
            }
        }
    };
    const spawnChunkWorker = async () => {
        while (chunks.length < multipartThreads) {
            let buffer = Buffer.alloc(partSize);
            const bytesRead = await read(fd, buffer, partSize); // Do we need to check for rejected promise?
            if (bytesRead === 0) {
                break;
            }
            totalBytesRead += bytesRead;
            buffer = buffer.subarray(0, bytesRead);
            const sha256 = createHash('sha256').update(buffer).digest();
            chunkChecksums.push(sha256);
            const md5 = createHash('md5').update(buffer).digest().toString('base64');
            enqueue(chunks, [partNumber, buffer, sha256.toString('base64'), md5]);
            partNumber++;
            if (runningUploadWorkers < multipartThreads) {
                runningUploadWorkers++;
                spawnUploadWorker();
            }
        }
        chunkWorkerRunning = false;
        if (runningUploadWorkers === 0) {
            partsCompletedCallback();
        }
    };
    const partsCompletedCallback = async () => {
        close(fd);
        if (totalBytesRead !== size) {
            try {
                await requestWrapper(async () => {
                    await abortMultipartUpload(client, multipartUploadJob);
                });
                throw new Error('Failed to read file: ' + localPath);
            } catch (e) {
                returnPromiseReject(e);
                return;
            }
        }
        let sha256 = createHash('sha256');
        for (const chunkChecksum of chunkChecksums) {
            sha256.update(chunkChecksum);
        }
        sha256 = sha256.digest().toString('base64');
        try {
            await requestWrapper(async () => {
                await completeMultipartUpload(client, multipartUploadJob, sha256);
            });
        } catch (e) {
            returnPromiseReject(e);
            return;
        }
        returnPromiseResolve();
    };
    runningMultipartUploads++;
    multipartThreads = Math.max(1, Math.floor(totalMultipartThreads / runningMultipartUploads));
    spawnChunkWorker();
    const releaseThread = () => {
        runningMultipartUploads--;
        if (runningMultipartUploads !== 0) {
            multipartThreads = Math.max(1, Math.floor(totalMultipartThreads / runningMultipartUploads));
        }
    };
    try {
        await returnPromise;
    } catch (e) {
        releaseThread();
        throw e;
    }
    releaseThread();
}

async function requestWrapper(request) {
    let retryCount = 1;
    while (true) {
        try {
            return await request();
        } catch (e) {
            if (retryCount >= 5 || (e['$metadata'] && e['$metadata'].httpStatusCode < 500)) {
                throw e;
            }
            await sleep(retryCount * 1000);
            retryCount++;
        }
    }
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function localToRelativePath(localPath, localRoot) {
    let relativePath = localPath.substring(localRoot.length).split(path.sep).join(path.posix.sep);
    if (relativePath.startsWith('/')) {
        relativePath = relativePath.substring(1);
    }
    return relativePath;
}

function getMime(matchPath) {
    for (const [ext, type] of mime) {
        if (ext.test(matchPath)) {
            return type;
        }
    }
    for (const [ext, type] of MIME) {
        if (ext.test(matchPath)) {
            return type;
        }
    }
    return undefined;
}