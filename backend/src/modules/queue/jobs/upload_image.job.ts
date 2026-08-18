import { Job } from "bullmq";
import { mediaFacade } from "#@/modules/media/media.facade.js";
import { socketManager } from "#@/infrastructure/websocket/socket-manager.js";

/**
 * Background job handler for image uploading and compression.
 * Used when the system is under high load to defer image processing (WebP conversion).
 * Emits a socket event to notify the user upon success.
 * @param job The BullMQ job containing user ID, file buffer, name, and MIME type.
 */
export const handleUploadImage = async (job: Job) => {
    const { userID, file_buffer, file_name, mime_type } = job.data;
    
    // Reconstruct buffer from JSON representation if needed
    const buffer = Buffer.from(file_buffer.data || file_buffer);
    
    console.log(`[QueueWorker] Compressing and uploading images for user ${userID}...`);
    const url = await mediaFacade.uploadImage(buffer, file_name, mime_type);
    
    console.log(`[QueueWorker] Upload successful, fire socket to notify user ${userID}`);
    socketManager.emitToUser(userID, "image_uploaded_success", { resource_url: url });
};
