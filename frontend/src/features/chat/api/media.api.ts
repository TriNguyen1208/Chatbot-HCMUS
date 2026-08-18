import { api } from "@/lib/api";

export const mediaApi = {
    uploadImage: async (file: File) => {
        const formData = new FormData();
        formData.append("file", file);
        const response = await api.post('/media/image', formData, {
            headers: {
                'Content-Type': 'multipart/form-data'
            }
        });
        return response.data;
    },

    initMultipartUpload: async (fileName: string, mimeType: string) => {
        const response = await api.post('/media/video/multipart/init', { fileName, mimeType });
        return response.data;
    },

    getPresignedUrlsForMultipart: async (fileKey: string, uploadId: string, partNumbers: number[]) => {
        const response = await api.post('/media/video/multipart/urls', { fileKey, uploadId, partNumbers });
        return response.data;
    },

    completeMultipartUpload: async (fileKey: string, uploadId: string, parts: { ETag: string; PartNumber: number }[]) => {
        const response = await api.post('/media/video/multipart/complete', { fileKey, uploadId, parts });
        return response.data;
    }
};
