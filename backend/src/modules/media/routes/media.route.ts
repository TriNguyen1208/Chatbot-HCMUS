import { Router } from "express";
import asyncHandler from "#@/shared/middlewares/asyncHandler.js";
import { AuthMiddleware } from "#@/shared/middlewares/auth.middleware.js";
import { uploadMiddleware } from "#@/shared/middlewares/upload.middleware.js";
import { mediaContainer } from "../media.container.js";
import { validate } from "#@/shared/middlewares/validate.middleware.js";
import { 
    InitMultipartUploadSchema, 
    GetPresignedUrlsSchema, 
    CompleteMultipartUploadSchema 
} from "../dto/media.dto.js";

const router = Router();

// API: Upload photos to Storage
router.post(
    "/image",
    AuthMiddleware.verifyAccessToken,
    uploadMiddleware,
    asyncHandler(mediaContainer.mediaController.uploadImage)
);

//Init Multipart upload
router.post(
    "/video/multipart/init",
    AuthMiddleware.verifyAccessToken,
    validate(InitMultipartUploadSchema),
    asyncHandler(mediaContainer.mediaController.initMultipartUpload)
);

// Get the list of presigned URL for each part (that is splitted in frontend)
router.post(
    "/video/multipart/urls",
    AuthMiddleware.verifyAccessToken,
    validate(GetPresignedUrlsSchema),
    asyncHandler(mediaContainer.mediaController.getPresignedUrlsForMultipart)
);

// Complete upload
router.post(
    "/video/multipart/complete",
    AuthMiddleware.verifyAccessToken,
    validate(CompleteMultipartUploadSchema),
    asyncHandler(mediaContainer.mediaController.completeMultipartUpload)
);

export default router;
