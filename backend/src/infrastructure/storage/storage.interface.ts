import { Buffer } from "node:buffer";

export interface IStorageService {
    uploadImage(fileBuffer: Buffer, fileName: string, mimeType: string): Promise<string>;
    deleteImage(imageUrl: string): Promise<void>;

    initMultipartUpload(fileName: string, mimeType: string): Promise<{ uploadId: string; fileKey: string }>;
    getPresignedUrlsForMultipart(fileKey: string, uploadId: string, partNumbers: number[]): Promise<string[]>;
    completeMultipartUpload(fileKey: string, uploadId: string, parts: { ETag: string; PartNumber: number }[]): Promise<string>;
    downloadFile(fileKey: string, destPath: string): Promise<void>;
    uploadFile(fileKey: string, filePath: string, mimeType: string): Promise<string>;
}