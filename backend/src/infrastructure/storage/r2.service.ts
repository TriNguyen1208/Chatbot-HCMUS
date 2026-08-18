import { S3Client, PutObjectCommand, DeleteObjectCommand, CreateMultipartUploadCommand, UploadPartCommand, CompleteMultipartUploadCommand, GetObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { type IStorageService } from "./storage.interface.js";
import { v4 as uuidv4 } from "uuid";
import { config } from "#@/config/config.js";
import fs from 'fs'

export class CloudflareR2Storage implements IStorageService {
    private s3Client: S3Client;
    private bucketName: string;
    constructor() {
        this.bucketName = config.cloudflare.bucket_name || "";
        this.s3Client = new S3Client({
            region: "auto",
            endpoint: `https://${config.cloudflare.account_id}.r2.cloudflarestorage.com`,
            credentials: {
                accessKeyId: config.cloudflare.access_key_id || "",
                secretAccessKey: config.cloudflare.secret_access_key || "",
            },
        });
    }

    /**
     * Uploads an image to R2 storage.
     * @param fileBuffer The image file buffer.
     * @param fileName The original file name.
     * @param mimeType The MIME type of the image (e.g., image/png).
     * @returns The public URL of the uploaded image.
     */
    async uploadImage(fileBuffer: Buffer, fileName: string, mimeType: string): Promise<string> {
        const uniqueFileName = `${uuidv4()}-${fileName}`;
        const command = new PutObjectCommand({
            Bucket: this.bucketName,
            Key: uniqueFileName,
            Body: fileBuffer,
            ContentType: mimeType,
        });
        await this.s3Client.send(command);
        const publicUrl = config.cloudflare.public_url;
        return `${publicUrl}/${uniqueFileName}`;
    }

    /**
     * Deletes an image from R2 storage.
     * @param imageUrl The full public URL of the image to delete.
     */
    async deleteImage(imageUrl: string): Promise<void> {
        try {
            const publicUrl = config.cloudflare.public_url || "";
            const key = imageUrl.replace(`${publicUrl}/`, "");
            const command = new DeleteObjectCommand({
                Bucket: this.bucketName,
                Key: key,
            });
            await this.s3Client.send(command);
            console.log(`[Storage] Cleaned junk files on Cloudflare: ${key}`);
        } catch (error) {
            console.error(`[Storage] File deletion failed: ${imageUrl}`, error);
        }
    }

    /**
     * Initiates a multipart upload on R2 storage.
     * @param fileName The original file name.
     * @param mimeType The MIME type of the file (e.g., video/mp4).
     * @returns An object containing the generated fileKey and the uploadId.
     */
    async initMultipartUpload(
        fileName: string, 
        mimeType: string
    ): Promise<{ uploadId: string; fileKey: string }> {
        const uniqueFileName = `${uuidv4()}-${fileName}`;
        const command = new CreateMultipartUploadCommand({
            Bucket: this.bucketName,
            Key: uniqueFileName,
            ContentType: mimeType,
        });        
        const response = await this.s3Client.send(command);        
        return {
            uploadId: response.UploadId!,
            fileKey: uniqueFileName
        };
    }
    
    /**
     * Generates presigned URLs for uploading specific parts of a multipart upload.
     * @param fileKey The key of the file in storage.
     * @param uploadId The multipart upload ID.
     * @param partNumbers An array of part numbers to generate URLs for.
     * @returns An array of presigned URLs corresponding to the part numbers.
     */
    async getPresignedUrlsForMultipart(fileKey: string, uploadId: string, partNumbers: number[]): Promise<string[]> {
        const presignedUrlPromises = partNumbers.map(async (partNumber) => {
            // Create an UploadPartCommand for each part number
            const command = new UploadPartCommand({
                Bucket: this.bucketName, 
                Key: fileKey, 
                UploadId: uploadId, 
                PartNumber: partNumber,
            });
            return await getSignedUrl(this.s3Client, command, { expiresIn: 3600 });
        });
        return await Promise.all(presignedUrlPromises);
    }

    /**
     * Completes a multipart upload by unifying all uploaded chunks.
     * @param fileKey The key of the file in storage.
     * @param uploadId The multipart upload ID.
     * @param parts An array containing the ETag and PartNumber of each uploaded chunk.
     * @returns The public URL of the completely uploaded file.
     */
    async completeMultipartUpload(fileKey: string, uploadId: string, parts: { ETag: string; PartNumber: number }[]): Promise<string> {
        const sortedParts = parts.sort((a, b) => a.PartNumber - b.PartNumber);
        
        const command = new CompleteMultipartUploadCommand({
            Bucket: this.bucketName,
            Key: fileKey,
            UploadId: uploadId,
            MultipartUpload: {
                Parts: sortedParts
            }
        });
        
        await this.s3Client.send(command);
        
        const publicUrl = config.cloudflare.public_url;
        return `${publicUrl}/${fileKey}`;
    }

    /**
     * Downloads a file from R2 storage to a local path using a stream.
     * @param fileKey The key of the file to download.
     * @param destPath The local destination path to save the file.
     */
    async downloadFile(fileKey: string, destPath: string): Promise<void> {
        const command = new GetObjectCommand({
            Bucket: this.bucketName,
            Key: fileKey,
        });
        const response = await this.s3Client.send(command);
        const stream = response.Body as NodeJS.ReadableStream;
        
        const writeStream = fs.createWriteStream(destPath);
        
        return new Promise((resolve, reject) => {
            stream.pipe(writeStream)
                .on("error", reject)
                .on("finish", resolve);
        });
    }
    
    /**
     * Uploads a local file to R2 storage.
     * @param fileKey The destination key for the file in storage.
     * @param filePath The local path of the file to upload.
     * @param mimeType The MIME type of the file.
     * @returns The public URL of the uploaded file.
     */
    async uploadFile(
        fileKey: string, 
        filePath: string, 
        mimeType: string
    ): Promise<string> {
        const fileStream = fs.createReadStream(filePath);
        const command = new PutObjectCommand({
            Bucket: this.bucketName,
            Key: fileKey,
            Body: fileStream,
            ContentType: mimeType,
        });
        await this.s3Client.send(command);
        const publicUrl = config.cloudflare.public_url;
        return `${publicUrl}/${fileKey}`;
    }
}