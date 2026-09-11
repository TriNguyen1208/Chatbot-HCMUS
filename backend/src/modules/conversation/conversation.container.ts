import { ConversationRepository } from "./conversation.repository.js";
import { ConversationService } from "./conversation.service.js";
import { ConversationController } from "./conversation.controller.js";
import { mongoDB } from "#@/infrastructure/database/mongodb.connection.js";
import { messageFacade } from "#@/modules/message/message.facade.js";

class ConversationContainer {
    private _conversationRepo?: ConversationRepository;
    public get conversationRepo() {
        if (!this._conversationRepo) {
            this._conversationRepo = new ConversationRepository(mongoDB);
        }
        return this._conversationRepo;
    }
    private _conversationService?: ConversationService;
    public get conversationService() {
        if (!this._conversationService) {
            this._conversationService = new ConversationService(this.conversationRepo, messageFacade);
        }
        return this._conversationService;
    }

    private _conversationController?: ConversationController;
    public get conversationController() {
        if (!this._conversationController) {
            this._conversationController = new ConversationController(this.conversationService);
        }
        return this._conversationController;
    }
}

export const conversationContainer = new ConversationContainer();