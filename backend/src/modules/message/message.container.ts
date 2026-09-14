import { conversationFacade } from "#@/modules/conversation/conversation.facade.js";

import { MessageRepository } from "./message.repository.js";
import { MessageService } from "./message.service.js";
import { MessageController } from "./message.controller.js";
import { mongoDB } from "#@/infrastructure/database/mongodb.connection.js";
import { MessageCache } from "./message.cache.js";

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
            this._messageService = new MessageService(conversationFacade, this.messageRepo, this.messageCache);
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

    private _messageCache?: MessageCache;
    public get messageCache() {
        if (!this._messageCache) {
            this._messageCache = new MessageCache();
        }
        return this._messageCache;
    }
}

export const messageContainer = new MessageContainer();