import { apiResponse } from "#@/shared/utils/api-response.js";
import type { Request, Response, NextFunction } from "express";
import type { MediaService } from "../services/media.service.js";
import createHttpError from "http-errors";

export class MediaController {
    constructor(private readonly mediaService: MediaService) { }

    /**
     * Initializes a multipart upload session for large files (e.g., videos) on the storage service.
     * @param req The Express request object containing fileName and mimeType.
     * @param res The Express response object.
     * @param next The Express next middleware function.
     */
    initMultipartUpload = async (req: Request, res: Response, next: NextFunction) => {
        const { fileName, mimeType } = req.body;
        const result = await this.mediaService.initMultipartUpload(fileName, mimeType);
        return apiResponse.success(res, result);
    }

    /**
     * Handles the upload of an image file.
     * If the system is overloaded, queues the image for background processing.
     * @param req The Express request object containing the file and user details.
     * @param res The Express response object.
     * @param next The Express next middleware function.
     */
    uploadImage = async (req: Request, res: Response, next: NextFunction) => {
        const file = req.file;
        const userID = req.user!.userID;

        if (!file) {
            throw createHttpError(400, "No attached image files found")
        }

        const result = await this.mediaService.handleImageUpload(file, userID);

        if (result.status === 'queued') {
            return apiResponse.success(res, {}, {
                message: result.message
            })
        }

        return apiResponse.success(res, { resource_url: result.url });
    }

    /**
     * Generates pre-signed URLs for each part of a multipart upload.
     * @param req The Express request object containing fileKey, uploadId, and partNumbers.
     * @param res The Express response object.
     * @param next The Express next middleware function.
     */
    getPresignedUrlsForMultipart = async (req: Request, res: Response, next: NextFunction) => {
        const { fileKey, uploadId, partNumbers } = req.body;
        const urls = await this.mediaService.getPresignedUrlsForMultipart(fileKey, uploadId, partNumbers);
        return apiResponse.success(res, { urls });
    }

    /**
     * Completes a multipart upload after all parts have been uploaded.
     * Triggers post-processing jobs (like FFmpeg video compression) in the background.
     * @param req The Express request object containing fileKey, uploadId, and uploaded parts.
     * @param res The Express response object.
     * @param next The Express next middleware function.
     */
    completeMultipartUpload = async (req: Request, res: Response, next: NextFunction) => {
        const { fileKey, uploadId, parts } = req.body;
        const url = await this.mediaService.completeMultipartUpload(fileKey, uploadId, parts);
        
        return apiResponse.success(res, { resource_url: url });
    }
}
