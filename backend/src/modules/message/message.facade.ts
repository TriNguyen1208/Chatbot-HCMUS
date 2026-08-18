import { messageContainer } from "./message.container.js";

// MessageFacade provides functions for other modules (such as Media) to communicate with Message
export class MessageFacade {
    private get messageService() {
        return messageContainer.messageService;
    }

    async handleVideoReady(fileKey: string, streamUrl: string, thumbnailUrl: string) {
        return await this.messageService.handleVideoReady(fileKey, streamUrl, thumbnailUrl);
    }

    async createSystemMessage(conversationId: string, content: string) {
        return this.messageService.createSystemMessage(conversationId, content);
    }
}
export const messageFacade = new MessageFacade();