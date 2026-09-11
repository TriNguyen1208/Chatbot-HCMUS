import type { Job } from "bullmq";
import { conversationFacade } from "#@/modules/conversation/conversation.facade.js";

export interface SyncWatermarkJobPayload {
    conversationId: string;
    userId: string;
    messageId: string;
    type: 'delivered' | 'read';
}

export const handleSyncWatermark = async (job: Job<SyncWatermarkJobPayload>) => {
    const { conversationId, userId, messageId, type } = job.data;
    await conversationFacade.updateWatermark(conversationId, userId, messageId, type);
    console.log(`✅ [QueueWorker] Successfully synced watermark for user ${userId} in conversation ${conversationId}`);
};
