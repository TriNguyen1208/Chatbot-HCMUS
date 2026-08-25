import { ConversationRepository } from "./repositories/conversation.repository.js";
import { ConversationService } from "./services/conversation.service.js";
import { ConversationController } from "./controllers/conversation.controller.js";
import { mongoDB } from "#@/infrastructure/database/mongoDBAtlas.js";
import { supabaseDB } from "#@/infrastructure/database/supabaseClient.js";
import { messageFacade } from "#@/modules/message/message.facade.js";

class ConversationContainer {
    public conversationRepo = new ConversationRepository(mongoDB);
    public conversationService = new ConversationService(this.conversationRepo, messageFacade);
    public conversationController = new ConversationController(this.conversationService);
}

export const conversationContainer = new ConversationContainer();