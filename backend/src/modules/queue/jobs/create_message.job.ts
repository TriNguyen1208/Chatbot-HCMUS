import { Job } from "bullmq";
import { messageContainer } from "#@/modules/message/message.container.js";
import { socketManager } from "#@/infrastructure/websocket/socket-manager.js";
import { conversationFacade } from "#@/modules/conversation/conversation.facade.js";

/**
 * Background job handler for creating messages.
 * Used when the system is under high load to defer database writes.
 * @param job The BullMQ job containing message data.
 */
export const handleCreateMessage = async (job: Job) => {
    const messageData = job.data;
    
    console.log(`[QueueWorker] Saving messages to DB for conversation ${messageData.conversation_id}...`);
    const savedMessage = await messageContainer.messageRepo.create(messageData);
    
    console.log(`[QueueWorker] Saved successfully, fired socket to notify group ${messageData.conversation_id}`);
    const members = await conversationFacade.getConversation(messageData.conversation_id);
    socketManager.emitToUsers(members, "new_message", savedMessage);
};
