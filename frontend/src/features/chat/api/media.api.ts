import { http } from "@/lib/api";

export interface UploadImageResult {
    url: string;
    fileKey: string;
}

export interface InitMultipartResult {
    uploadId: string;
    fileKey: string;
}

export interface PresignedUrlsResult {
    urls: string[];
}

export const mediaApi = {
    //Upload 1 image
    uploadImage: async (file: File): Promise<UploadImageResult> => {
        const formData = new FormData();
        formData.append("file", file);
        return http.post<UploadImageResult>("/media/image", formData, {
            headers: {
                "Content-Type": "multipart/form-data",
            },
        });
    },

    //Get uploadId and fileKey (init in S3)
    initMultipartUpload: async (
        fileName: string,
        mimeType: string,
    ): Promise<InitMultipartResult> => {
        return http.post<InitMultipartResult>("/media/video/multipart/init", {
            fileName,
            mimeType,
        });
    },

    //Get presigned url (from S3)
    getPresignedUrlsForMultipart: async (
        fileKey: string,
        uploadId: string,
        partNumbers: number[],
    ): Promise<PresignedUrlsResult> => {
        return http.post<PresignedUrlsResult>("/media/video/multipart/urls", {
            fileKey,
            uploadId,
            partNumbers,
        });
    },

    //Complete upload
    completeMultipartUpload: async (
        fileKey: string,
        uploadId: string,
        parts: { ETag: string; PartNumber: number }[],
    ): Promise<any> => {
        return http.post<any>("/media/video/multipart/complete", {
            fileKey,
            uploadId,
            parts,
        });
    },
};
