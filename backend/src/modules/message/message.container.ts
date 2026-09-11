import { conversationFacade } from "#@/modules/conversation/conversation.facade.js";

import { MessageRepository } from "./message.repository.js";
import { MessageService } from "./message.service.js";
import { MessageController } from "./message.controller.js";
import { mongoDB } from "#@/infrastructure/database/mongodb.connection.js";

class MessageContainer {
    private _messageRepo?: MessageRepository;
    public get messageRepo() {
        if (!this._messageRepo) {
            this._messageRepo = new MessageRepository(mongoDB);
        }
        return this._messageRepo;
    }
    
    private _messageService?: MessageService;
    public get messageService() {
        if (!this._messageService) {
            this._messageService = new MessageService(conversationFacade, this.messageRepo);
        }
        return this._messageService;
    }
    
    private _messageController?: MessageController;
    public get messageController() {
        if (!this._messageController) {
            this._messageController = new MessageController(this.messageService);
        }
        return this._messageController;
    }
}

export const messageContainer = new MessageContainer();