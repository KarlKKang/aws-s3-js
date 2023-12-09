import { getObjectChecksum } from './helper/getObjectChecksum.js';
import { S3Client } from '@aws-sdk/client-s3';
import util from 'util';

function printHelp() {
    console.error('Usage:');
    console.error('node get_object_checksum.js <BUCKET> <OBJECT_KEY> [OPTIONS]');
    console.error();
    console.error('Options:');
    console.error('--region <REGION>');
    console.error('    Use the given region instead of the default region.');
    console.error('--no-colors');
    console.error('    Disable color output.');
}

const bucket = process.argv[2];
const objectKey = process.argv[3];
if (!bucket || !objectKey) {
    console.error('Error: Missing required argument');
    console.error();
    printHelp();
    process.exit(1);
}

let region = undefined;
let colors = true;
for (let i = 4; i < process.argv.length; i++) {
    if (process.argv[i] === '--region') {
        region = process.argv[++i];
        if (!region) {
            console.error('Error: Expected argument after --region');
            console.error();
            printHelp();
            process.exit(1);
        }
    } else if (process.argv[i] === '--no-colors') {
        colors = false;
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
const results = await getObjectChecksum(client, bucket, objectKey);
console.log(util.inspect(results, { depth: null, colors: colors, maxArrayLength: Infinity }));
