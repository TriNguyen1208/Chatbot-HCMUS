import { queueService } from "#@/background/queue.service.js";

export enum SyncOperation {
    CREATE = 'CREATE',
    UPDATE = 'UPDATE',
    DELETE = 'DELETE'
}

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

        // Dispatch background job to BullMQ for Elasticsearch synchronization
        queueService.addJob('sync_es', {
            index: collection,
            operation,
            data: payloadData
        });
    };

    if (Array.isArray(data)) {
        data.forEach(processData);
    } else {
        processData(data);
    }
};
