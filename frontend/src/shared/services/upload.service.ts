import { mediaApi } from "@/features/chat/api/media.api";

export interface MultipartUploadResult {
  fileKey: string;
  resourceUrl?: string;
}

export const uploadService = {
  uploadImage: async (file: File): Promise<{ url: string; fileKey: string }> => {
    const res = await mediaApi.uploadImage(file);
    const url = (res as any)?.resource_url || res.url;
    return { url, fileKey: res.fileKey };
  },

  uploadVideoMultipart: async (
    file: File,
    onProgress?: (percent: number) => void
  ): Promise<MultipartUploadResult> => {
    const CHUNK_SIZE = 5 * 1024 * 1024; // 5MB chunks
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);

    const initRes = await mediaApi.initMultipartUpload(file.name, file.type);
    const { uploadId, fileKey } = initRes;

    const partNumbers = Array.from({ length: totalChunks }, (_, i) => i + 1);
    const urlRes = await mediaApi.getPresignedUrlsForMultipart(fileKey, uploadId, partNumbers);
    const presignedUrls = urlRes.urls;

    const uploadedParts: { ETag: string; PartNumber: number }[] = [];
    let completedChunks = 0;

    const uploadPromises = partNumbers.map(async (partNumber, index) => {
      const start = (partNumber - 1) * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, file.size);
      const chunk = file.slice(start, end);
      const presignedUrl = presignedUrls[index];

      const uploadRes = await fetch(presignedUrl, {
        method: "PUT",
        body: chunk,
      });

      const eTag = uploadRes.headers.get("ETag")?.replace(/"/g, "") || "";
      uploadedParts.push({ ETag: eTag, PartNumber: partNumber });

      completedChunks++;
      if (onProgress) {
        onProgress(Math.round((completedChunks / totalChunks) * 100));
      }
    });

    await Promise.all(uploadPromises);

    const completeRes = await mediaApi.completeMultipartUpload(fileKey, uploadId, uploadedParts);
    const resourceUrl = completeRes?.data?.resource_url || completeRes?.resource_url;

    return { fileKey, resourceUrl };
  },
};
