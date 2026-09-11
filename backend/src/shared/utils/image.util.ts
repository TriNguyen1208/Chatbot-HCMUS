import sharp from "sharp";
import { Buffer } from "node:buffer";

// Separate the image compression logic into a separate function so that the Service does not bloat
export const compressImage = async (fileBuffer: Buffer, mimeType: string): Promise<Buffer> => {
    // Only compress if the format is an image
    if (mimeType.startsWith('image/')) {
        return await sharp(fileBuffer)
            .resize({ width: 1200, withoutEnlargement: true }) // Lock the maximum width to 1200px
            .jpeg({ quality: 80 }) // Standard JPEG compression keeps 80% quality
            .toBuffer();
    }
    return fileBuffer;
};