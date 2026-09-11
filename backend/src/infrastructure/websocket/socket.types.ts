export interface TypingPayload {
    conversationId: string;
    receiverIds: string[];
    name?: string;
}

export interface StopTypingPayload {
    conversationId: string;
    receiverIds: string[];
}

export interface MarkWatermarkPayload {
    conversationId: string;
    messageId: string;
}

export type WatermarkType = 'delivered' | 'read';

export interface WatermarkUpdatedBroadcast {
    conversationId: string;
    userId: string;
    messageId: string;
    type: WatermarkType;
}

export interface UserPresenceBroadcast {
    userId: string;
    last_active?: Date;
}
