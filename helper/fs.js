import { promises as fs } from 'fs';
import path from 'path';

export async function scanDir(fullDirPath) {
    const result = [];
    const items = await fs.readdir(fullDirPath, { withFileTypes: true });
    for (const item of items) {
        const fullPath = path.join(fullDirPath, item.name);
        if (item.isDirectory()) {
            const subdirResult = await scanDir(fullPath);
            result.push(...subdirResult);
        } else {
            result.push(fullPath);
        }
    }
    return result;
}

export async function open(path) {
    return await fs.open(path, 'r');
}

export async function close(fd) {
    return await fd.close();
}

export async function read(fd, buffer, length) {
    return (await fd.read(buffer, 0, length, null)).bytesRead;
}

export async function getFileAttributes(path) {
    const stat = await fs.stat(path);
    return {
        isFile: stat.isFile(),
        isDirectory: stat.isDirectory(),
        size: stat.size,
        mtime: stat.mtime,
    };
}