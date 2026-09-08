import { rabbitmq } from './index.js';
import { esClient } from '../elasticsearch/index.js';
import { type SyncPayload, SyncOperation } from './types.js';

const processSyncEvent = async (index: string, payload: SyncPayload<any>) => {
    const { operation, data } = payload;
    const documentId = data.id || data._id?.toString();

    if (!documentId) {
        console.warn(`[RabbitMQ Consumer] Missing document ID for operation ${operation} on index ${index}`);
        return; 
    }

    try {
        switch (operation) {
            case SyncOperation.CREATE:
            case SyncOperation.UPDATE:
                
                const { _id, ...docData } = data;
                
                await esClient.index({
                    index,           
                    id: documentId,  
                    body: {
                        id: documentId,
                        ...docData   
                    }
                });
                break;

            case SyncOperation.DELETE:
                await esClient.delete({
                    index,
                    id: documentId,
                });
                break;
                
            default:
                console.warn(`[RabbitMQ Consumer] Unknown operation ${operation}`);
        }
        
        console.log(`✅ [RabbitMQ Consumer] Successfully synced document ${documentId} to index ${index} (Operation: ${operation})`);
    } catch (error: any) {
        if (error.meta?.body?.error?.type === 'not_found' && operation === SyncOperation.DELETE) {
            return;
        }
        console.error(`[RabbitMQ Consumer] Failed to process ES sync for ${index}:`, error);
        throw error;
    }
};

export const startConsumers = async () => {
    try {
        const channel = rabbitmq.getChannel();

        const setupConsumer = (queueName: string, indexName: string) => {
            channel.consume(queueName, async (msg) => {
                if (msg !== null) {
                    try {
                        const payload: SyncPayload<any> = JSON.parse(msg.content.toString());
                        
                        await processSyncEvent(indexName, payload);
            
                        channel.ack(msg);
                    } catch (error) {
                        console.error(`[RabbitMQ Consumer] Error processing message from ${queueName}:`, error);
                        channel.nack(msg, false, false);
                    }
                }
            });
            console.log(`✅ Started consumer for queue: ${queueName}`);
        };

        setupConsumer('sync_user_es', 'users');
        setupConsumer('sync_conversation_es', 'conversations');
        setupConsumer('sync_message_es', 'messages');

        // Special consumer for DB watermarks
        channel.consume('sync_watermark_db', async (msg) => {
            if (msg !== null) {
                try {
                    const payload: SyncPayload<{ conversationId: string, userId: string, messageId: string, type: 'delivered' | 'read' }> = JSON.parse(msg.content.toString());
                    const { conversationId, userId, messageId, type } = payload.data;
                    
                    // Import dynamically to avoid circular dependencies
                    const { conversationFacade } = await import('#@/modules/conversation/conversation.facade.js');
                    await conversationFacade.updateWatermark(conversationId, userId, messageId, type);

                    console.log(`✅ [RabbitMQ Consumer] Successfully synced watermark for user ${userId} in conversation ${conversationId}`);
                    channel.ack(msg);
                } catch (error) {
                    console.error(`[RabbitMQ Consumer] Error processing watermark message:`, error);
                    channel.nack(msg, false, false);
                }
            }
        });
        console.log(`✅ Started consumer for queue: sync_watermark_db`);

    } catch (error) {
        console.error('❌ Failed to start RabbitMQ consumers:', error);
    }
};
