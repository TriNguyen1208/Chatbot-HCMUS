import { rabbitmq } from './index.js'; 
import { type SyncPayload, SyncOperation } from './types.js';

// Hàm core dùng để đẩy message vào 1 queue bất kỳ
export const publishToQueue = async <T>(queueName: string, payload: SyncPayload<T>) => {
    try {
        const channel = rabbitmq.getChannel(); 
        
        const success = channel.sendToQueue(queueName, Buffer.from(JSON.stringify(payload)), {
            persistent: true
        });
        
        if (!success) {
            console.warn(`[RabbitMQ] Queue ${queueName} is full, message dropped or delayed.`);
        }
    } catch (error) {
        console.error(`[RabbitMQ] Failed to publish message to queue ${queueName}:`, error);
    }
};

export const syncUserES = (payload: SyncPayload<any>) => publishToQueue('sync_user_es', payload);
export const syncConversationES = (payload: SyncPayload<any>) => publishToQueue('sync_conversation_es', payload);
export const syncMessageES = (payload: SyncPayload<any>) => publishToQueue('sync_message_es', payload);
export const syncWatermarkToDB = (payload: { conversationId: string, userId: string, messageId: string, type: 'delivered' | 'read' }) => publishToQueue('sync_watermark_db', { operation: SyncOperation.UPDATE, data: payload } as any);
