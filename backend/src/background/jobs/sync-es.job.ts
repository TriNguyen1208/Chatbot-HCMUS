import type { Job } from "bullmq";
import { esClient } from "#@/infrastructure/elasticsearch/es.client.js";
import { SyncOperation } from "#@/shared/utils/sync.util.js";

export interface SyncESJobPayload {
    index: string;
    operation: SyncOperation;
    data: any;
}

export const handleSyncES = async (job: Job<SyncESJobPayload>) => {
    const { index, operation, data } = job.data;
    const documentId = data.id || data._id?.toString();

    if (!documentId) {
        console.warn(`[QueueWorker] Missing document ID for operation ${operation} on index ${index}`);
        return;
    }

    try {
        switch (operation) {
            case SyncOperation.CREATE:
            case SyncOperation.UPDATE: {
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
            }

            case SyncOperation.DELETE:
                await esClient.delete({
                    index,
                    id: documentId
                });
                break;

            default:
                console.warn(`[QueueWorker] Unknown operation ${operation}`);
        }

        console.log(`✅ [QueueWorker] Successfully synced document ${documentId} to index ${index} (Operation: ${operation})`);
    } catch (error: any) {
        if (error.meta?.body?.error?.type === 'not_found' && operation === SyncOperation.DELETE) {
            return;
        }
        console.error(`[QueueWorker] Failed to process ES sync for ${index}:`, error);
        throw error;
    }
};
