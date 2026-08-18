import { conversationFacade } from "#@/modules/conversation/conversation.facade.js";

import { MessageRepository } from "./repositories/message.repository.js";
import { MessageService } from "./services/message.service.js";
import { MessageController } from "./controllers/message.controller.js";
import { mongoDB } from "#@/infrastructure/database/mongoDBAtlas.js";
import { supabaseDB } from "#@/infrastructure/database/supabaseClient.js";

class MessageContainer {
    public messageRepo = new MessageRepository(mongoDB, supabaseDB);
    
    public messageService = new MessageService(
        conversationFacade, 
        this.messageRepo
    );
    
    public messageController = new MessageController(this.messageService);
}

export const messageContainer = new MessageContainer();