import { syncUserES, syncConversationES, syncMessageES } from "#@/infrastructure/rabbitmq/producer.js";
import { SyncOperation } from "#@/infrastructure/rabbitmq/types.js";

const ES_MAPPINGS: Record<string, string[]> = {
    users: ['id', 'mssv', 'name', 'email', 'phone'],
    conversations: ['id', 'name', 'member_ids'],
    messages: ['id', 'conversation_id', 'sender_id', 'content']
};

export const triggerSync = (collection: string, operation: SyncOperation, data: any) => {
    const processData = (doc: any) => {
        if (!doc) return;
        
        // Native driver returns _id, map it to id if necessary
        const mappedId = doc.id || (doc._id ? doc._id.toString() : undefined);
        const docWithId = { ...doc, id: mappedId };
        
        const payloadData: any = {};
        const allowedFields = ES_MAPPINGS[collection] || [];
        
        allowedFields.forEach(field => {
            if (docWithId[field] !== undefined) {
                payloadData[field] = docWithId[field];
            }
        });

        switch (collection) {
            case 'users':
                syncUserES({ operation, data: payloadData });
                break;
            case 'conversations':
                syncConversationES({ operation, data: payloadData });
                break;
            case 'messages':
                syncMessageES({ operation, data: payloadData });
                break;
        }
    };

    if (Array.isArray(data)) {
        data.forEach(processData);
    } else {
        processData(data);
    }
};
