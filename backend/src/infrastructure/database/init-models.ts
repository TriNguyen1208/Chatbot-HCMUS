import { UserModel } from "#@/modules/user/user.entity.js";
import { ConversationModel } from "#@/modules/conversation/conversation.entity.js";
import { MessageModel } from "#@/modules/message/message.entity.js";
import { KeyStoreModel } from "#@/modules/auth/keystore.entity.js";

export const initializeDatabaseModels = async (): Promise<void> => {
    try {
        await Promise.all([
            UserModel.createCollection(),
            ConversationModel.createCollection(),
            MessageModel.createCollection(),
            KeyStoreModel.createCollection(),
        ]);
        console.log("✅ MongoDB collections & indexes ensured successfully");
    } catch (err: any) {
        if (err.code !== 48) { // Ignore NamespaceExists error (code 48)
            console.error("❌ Error ensuring MongoDB collections:", err);
            throw err;
        }
    }
};
