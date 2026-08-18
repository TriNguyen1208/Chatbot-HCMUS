import { type IStorageService } from "#@/infrastructure/storage/storage.interface.js";
import createHttpError from "http-errors";
import sharp from "sharp";
import { queueService } from "#@/modules/queue/queue.service.js";
import { checkSystemLoad } from "#@/shared/utils/system-monitor.js";

export class MediaService {
    constructor(private readonly storageService: IStorageService) { }

    /**
     * Processes an image buffer (compresses and converts to WebP) and uploads it to storage.
     * @param fileBuffer The raw image buffer.
     * @param originalName The original file name.
     * @param mimeType The file's MIME type.
     * @returns The public URL of the uploaded image.
     */
    async processAndUploadImage(
        fileBuffer: Buffer,
        originalName: string,
        mimeType: string
    ): Promise<string> {
        let finalBuffer = fileBuffer;

        if (mimeType.startsWith('image/')) {
            finalBuffer = await sharp(fileBuffer)
                .webp({ quality: 80 })
                .toBuffer();

            originalName = originalName.replace(/\.[^/.]+$/, "") + ".webp";
            mimeType = "image/webp";
        }
        const url = await this.storageService.uploadImage(finalBuffer, originalName, mimeType);

        return url;
    }

    /**
     * Handles an incoming image upload request.
     * Checks system load and either processes the image immediately or queues it for later.
     * @param file The uploaded file object from Multer.
     * @param userID The ID of the user uploading the file.
     * @returns An object indicating the upload status ('success' or 'queued') and the resulting URL/message.
     */
    async handleImageUpload(file: Express.Multer.File, userID: string) {
        const isOverloaded = await checkSystemLoad();
        if (isOverloaded) {
            console.warn(`[MediaService] The system is busy. Push images to queue for user ${userID}`);
            await queueService.addJob('upload_image', {
                userID,
                file_buffer: file.buffer.toJSON(),
                file_name: file.originalname,
                mime_type: file.mimetype
            });
            return {
                status: 'queued',
                message: "The system is busy, images are being processed in the background."
            };
        }

        const url = await this.processAndUploadImage(file.buffer, file.originalname, file.mimetype)
        return {
            status: 'success',
            url
        };
    }


    /**
     * Initializes a multipart upload on the storage service.
     * @param fileName The name of the file to be uploaded.
     * @param mimeType The MIME type of the file.
     * @returns The upload ID and file key.
     */
    async initMultipartUpload(fileName: string, mimeType: string) {
        return await this.storageService.initMultipartUpload(fileName, mimeType);
    }

    /**
     * Retrieves pre-signed URLs for uploading multiple parts of a file.
     * @param fileKey The unique key of the file in storage.
     * @param uploadId The active multipart upload ID.
     * @param partNumbers An array of part numbers to generate URLs for.
     * @returns A list of pre-signed URLs.
     */
    async getPresignedUrlsForMultipart(fileKey: string, uploadId: string, partNumbers: number[]) {
        return await this.storageService.getPresignedUrlsForMultipart(fileKey, uploadId, partNumbers);
    }

    /**
     * Completes a multipart upload and queues a background job for video processing.
     * @param fileKey The unique key of the file in storage.
     * @param uploadId The active multipart upload ID.
     * @param parts An array of uploaded parts containing ETags and PartNumbers.
     * @returns The public URL of the uploaded file.
     */
    async completeMultipartUpload(fileKey: string, uploadId: string, parts: { ETag: string; PartNumber: number }[]) {
        const url = await this.storageService.completeMultipartUpload(fileKey, uploadId, parts);
        
        //After upload successfully, push job in BullMQ to process FFmpeg
        await queueService.addJob('process_video', { fileKey });

        return url;
    }
    /**
     * Downloads a file from the storage service to a local destination path.
     * @param fileKey The unique key of the file in storage.
     * @param destPath The local path where the file should be saved.
     */
    async downloadFile(fileKey: string, destPath: string): Promise<void>{
        await this.storageService.downloadFile(fileKey, destPath)
    }

    /**
     * Uploads a local file to the storage service.
     * @param fileKey The unique key to save the file as.
     * @param filePath The local path of the file to upload.
     * @param mimeType The MIME type of the file.
     * @returns The public URL of the uploaded file.
     */
    async uploadFile(fileKey: string, filePath: string, mimeType: string): Promise<string> {
        return await this.storageService.uploadFile(fileKey, filePath, mimeType)
    }
}
