import { S3Client } from '@aws-sdk/client-s3';
import { listObjectsV2 } from './helper/listObjectsV2.js';
import { scanDir } from './helper/fs.js';
import path from 'path';
import { verifyFile } from './helper/verifyFile.js';
import { getObjectChecksum } from './helper/getObjectChecksum.js';
import { createQueue, dequeue, enqueue } from './helper/queue.js';

function printHelp() {
    console.error('Usage:');
    console.error(process.argv0 + ' ' + process.argv[1] + ' <BUCKET> <REMOTE_ROOT> <LOCAL_ROOT> [OPTIONS]');
    console.error();
    console.error('Options:');
    console.error('--region <REGION>');
    console.error('    Use the given region instead of the default region.');
    console.error('--threads <COUNT>');
    console.error('    Compute the checksums of the given number of files concurrently. Default: 512. Reduce this number if you are using drives with low random I/O performance.');
    console.error('--network-threads <COUNT>');
    console.error('    Use the given number of concurrent network requests. Default: 16.');
}

const bucket = process.argv[2];
let remoteRoot = process.argv[3];
let localRoot = process.argv[4];
if (!bucket || !remoteRoot || !localRoot) {
    console.error('Error: Missing required argument');
    console.error();
    printHelp();
    process.exit(1);
}

let region = undefined;
let threads = 512;
let networkThreads = 16;
for (let i = 5; i < process.argv.length; i++) {
    if (process.argv[i] === '--region') {
        region = process.argv[++i];
        if (!region) {
            console.error('Error: Expected argument after --region');
            console.error();
            printHelp();
            process.exit(1);
        }
    } else if (process.argv[i] === '--threads') {
        threads = parseInt(process.argv[++i]);
        if (isNaN(threads) || threads <= 0) {
            console.error('Error: Expected positive integer after --threads');
            console.error();
            printHelp();
            process.exit(1);
        }
    } else if (process.argv[i] === '--network-threads') {
        networkThreads = parseInt(process.argv[++i]);
        if (isNaN(networkThreads) || networkThreads <= 0) {
            console.error('Error: Expected positive integer after --network-threads');
            console.error();
            printHelp();
            process.exit(1);
        }
    } else {
        console.error('Error: Unknown option: ' + process.argv[i]);
        console.error();
        printHelp();
        process.exit(1);
    }
}

localRoot = path.resolve(localRoot);

if (remoteRoot.startsWith('/')) {
    remoteRoot = remoteRoot.substring(1);
}
if (remoteRoot !== '' && !remoteRoot.endsWith('/')) {
    remoteRoot += '/';
}

const client = new S3Client({
    region: region,
});
const localPathsPromise = scanDir(localRoot);
const remoteFiles = await listObjectsV2(client, bucket, remoteRoot);

const pendingRemotePaths = createQueue();
for (const remotePath in remoteFiles) {
    enqueue(pendingRemotePaths, remotePath);
}

const networkWorkers = [];
for (let i = 0; i < networkThreads; i++) {
    networkWorkers.push((async () => {
        while (pendingRemotePaths.length !== 0) {
            const remotePath = dequeue(pendingRemotePaths);
            let retryCount = 1;
            while (true) {
                try {
                    remoteFiles[remotePath] = await getObjectChecksum(client, bucket, remotePath);
                    break;
                } catch (e) {
                    if (retryCount >= 5 || (e['$metadata'] && e['$metadata'].httpStatusCode < 500)) {
                        throw e;
                    }
                    retryCount++;
                    await new Promise((resolve) => {
                        setTimeout(resolve, retryCount * 1000);
                    });
                }
            }
        }
    })());
}

const [localPaths] = await Promise.all([localPathsPromise, ...networkWorkers]);
const pendingLocalPaths = createQueue();
for (const localPath of localPaths) {
    enqueue(pendingLocalPaths, localPath);
}

let totalPassed = 0;
let totalMismatch = 0;
let totalMissing = 0;
let totalSkipped = 0;

const workers = [];
for (let i = 0; i < threads; i++) {
    workers.push((async () => {
        while (pendingLocalPaths.length !== 0) {
            const localPath = dequeue(pendingLocalPaths);
            let relativePath = localPath.substring(localRoot.length).split(path.sep).join(path.posix.sep);
            if (relativePath.startsWith('/')) {
                relativePath = relativePath.substring(1);
            }
            const remotePath = remoteRoot + relativePath;
            const objectChecksum = remoteFiles[remotePath];
            if (!objectChecksum) {
                console.error(relativePath + ': Missing from remote.');
                totalMissing++;
                continue;
            }
            delete remoteFiles[remotePath];
            let verifyResult;
            try {
                verifyResult = await verifyFile(objectChecksum, localPath);
            } catch (e) {
                console.error(relativePath + ': ' + e.message);
                totalSkipped++;
                continue;
            }
            if (verifyResult) {
                console.log(relativePath + ': OK');
                totalPassed++;
            } else {
                console.error(relativePath + ': Checksums do not match.');
                totalMismatch++;
            }
        }
    })());
}
await Promise.all(workers);

for (const remotePath in remoteFiles) {
    console.error(remotePath + ': Missing from local.');
    totalMissing++;
}

console.log();
console.log('Total passed: ' + totalPassed);
console.log('Total mismatch: ' + totalMismatch);
console.log('Total missing (either local or remote): ' + totalMissing);
console.log('Total skipped (due to error): ' + totalSkipped);