import { listObjectsV2 } from './helper/listObjectsV2.js';
import { S3Client } from '@aws-sdk/client-s3';
import util from 'util';

function printHelp() {
    console.error('Usage:');
    console.error('node list.js <BUCKET> [OPTIONS]');
    console.error();
    console.error('Options:');
    console.error('--region <REGION>');
    console.error('    Use the given region instead of the default region.');
    console.error('--prefix <PREFIX>');
    console.error('    Only list objects with the given prefix.');
    console.error('--no-colors');
    console.error('    Disable color output.');
}

const bucket = process.argv[2];
if (!bucket) {
    console.error('Error: Missing required argument');
    console.error();
    printHelp();
    process.exit(1);
}

let region = undefined;
let prefix = '';
let colors = true;
for (let i = 3; i < process.argv.length; i++) {
    if (process.argv[i] === '--region') {
        region = process.argv[++i];
        if (!region) {
            console.error('Error: Expected argument after --region');
            console.error();
            printHelp();
            process.exit(1);
        }
    } else if (process.argv[i] === '--prefix') {
        prefix = process.argv[++i];
        if (!prefix) {
            console.error('Error: Expected argument after --prefix');
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
const results = await listObjectsV2(client, bucket, prefix);
console.log(util.inspect(results, { depth: null, colors: colors }));
console.log();
console.log('Total object count: ' + Object.keys(results).length);