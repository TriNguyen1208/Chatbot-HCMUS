import { Router } from "express";
import asyncHandler from "#@/shared/middlewares/async-handler.js";
import { AuthMiddleware } from "#@/shared/middlewares/auth.middleware.js";
import { uploadMiddleware } from "#@/shared/middlewares/upload.middleware.js";
import { mediaContainer } from "./media.container.js";
import { validate } from "#@/shared/middlewares/validate.middleware.js";
import { mediaRateLimiter } from "#@/shared/middlewares/rate-limit.middleware.js";
import { 
    InitMultipartUploadSchema, 
    GetPresignedUrlsSchema, 
    CompleteMultipartUploadSchema 
} from "./media.dto.js";

const router = Router();

// API: Upload photos to Storage
router.post(
    "/image",
    AuthMiddleware.verifyAccessToken,
    mediaRateLimiter,
    uploadMiddleware,
    asyncHandler(mediaContainer.mediaController.uploadImage)
);

//Init Multipart upload
router.post(
    "/video/multipart/init",
    AuthMiddleware.verifyAccessToken,
    mediaRateLimiter,
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
