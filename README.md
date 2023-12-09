# AWS-S3-JS

## Description

This project is an implementation of some high-level AWS S3 commands using Node.js. It focuses on providing extra data integrity by using additional checksums. The Javascript asynchroneous nature is also taken into consideration to provide better performance. 

## Commands

The following commands are implemented:

- `list`
- `upload`
- `get_object_checksum`
- `verify_file`
- `verify_dir`

The usage of each command can be found by running `node <command>.js` in the project root directory. 

Currently no `download` command is implemented, as it can simply achieved by using other tools like AWS CLI and then running `verify_file` or `verify_dir` to verify the integrity of the downloaded files.

These commands will use the credentials stored in `~/.aws/credentials` (can be [configured using AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html)). Currently there is no way to specify a different credential for each command.

### upload

This command will upload a file or a directory to a specified bucket. Additional SHA256 checksums will be calculated and uploaded along with the file. Local files with the same size and less recent modification time will be skipped, just like the `aws s3 sync` command.

### verify_file and verify_dir

These commands will verify the integrity of a file or a directory by comparing the SHA1/SHA256 checksums stored in the metadata of the S3 objects with the local checksums. CRC32 and CRC32C checksums are not supported. When verifying a directory, checksums of all objects will be retrieved from S3 first before any comparison is made. Therefore no Internet connection is required after the initial step.