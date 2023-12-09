import { getObjectChecksum } from './helper/getObjectChecksum.js';
import { verifyFile } from './helper/verifyFile.js';
import { S3Client } from '@aws-sdk/client-s3';

function printHelp() {
    console.error('Usage:');
    console.error('node checksum_file.js <BUCKET> <OBJECT_KEY> <LOCAL_FILE_PATH> [OPTIONS]');
    console.error();
    console.error('Options:');
    console.error('--region <REGION>');
    console.error('    Use the given region instead of the default region.');
}

const bucket = process.argv[2];
const objectKey = process.argv[3];
const localFilePath = process.argv[4];
if (!bucket || !objectKey || !localFilePath) {
    console.error('Error: Missing required argument');
    console.error();
    printHelp();
    process.exit(1);
}

let region = undefined;
for (let i = 5; i < process.argv.length; i++) {
    if (process.argv[i] === '--region') {
        region = process.argv[++i];
        if (!region) {
            console.error('Error: Expected argument after --region');
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

const client = new S3Client({
    region: region,
});
const objectChecksum = await getObjectChecksum(client, bucket, objectKey);
if (await verifyFile(objectChecksum, localFilePath)) {
    console.log('Checksums match.');
} else {
    console.error('Checksums do not match.');
}

