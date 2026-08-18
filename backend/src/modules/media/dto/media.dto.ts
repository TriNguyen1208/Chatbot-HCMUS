import { z } from "zod";

export const InitMultipartUploadSchema = z.object({
    body: z.object({
        fileName: z.string(),
        mimeType: z.string()
    })
});

export const GetPresignedUrlsSchema = z.object({
    body: z.object({
        fileKey: z.string(),
        uploadId: z.string(),
        partNumbers: z.array(z.number()).min(1, "partNumbers must not be empty")
    })
});

export const CompleteMultipartUploadSchema = z.object({
    body: z.object({
        fileKey: z.string(),
        uploadId: z.string(),
        parts: z.array(
            z.object({
                ETag: z.string(),
                PartNumber: z.number()
            })
        ).min(1, "parts must not be empty")
    })
});

export type InitMultipartUploadDto = z.infer<typeof InitMultipartUploadSchema>['body'];
export type GetPresignedUrlsDto = z.infer<typeof GetPresignedUrlsSchema>['body'];
export type CompleteMultipartUploadDto = z.infer<typeof CompleteMultipartUploadSchema>['body'];
