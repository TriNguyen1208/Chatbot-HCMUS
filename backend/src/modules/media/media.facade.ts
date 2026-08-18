import { mediaContainer } from "./media.container.js";


export class MediaFacade {
    private get mediaService() {
        return mediaContainer.mediaService;
    }
    async uploadImage(fileBuffer: Buffer, originalName: string, mimeType: string): Promise<string> {
        return await this.mediaService.processAndUploadImage(fileBuffer, originalName, mimeType);
    }

    async downloadFile(fileKey: string, destPath: string): Promise<void>{
        await this.mediaService.downloadFile(fileKey, destPath);
    }
    
    async uploadFile(fileKey: string, filePath: string, mimeType: string): Promise<string> {
        return await this.mediaService.uploadFile(fileKey, filePath, mimeType)
    }
    
}
export const mediaFacade = new MediaFacade()