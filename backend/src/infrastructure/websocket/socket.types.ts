export interface TypingPayload {
    conversationId: string;
    receiverIds?: string[];
    name?: string;
}

export interface StopTypingPayload {
    conversationId: string;
    receiverIds?: string[];
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

// --- Standard Socket.IO Acknowledgement Response Format ---
export interface SocketAckResponse<T = any> {
    success: boolean;
    data?: T;
    message?: string;
    code?: number;
}

// --- Message Mutation Payloads ---
export interface SendMessageSocketPayload {
    conversation_id?: string;
    receiver_id?: string;
    content?: string;
    type?: 'text' | 'file' | 'link' | 'image' | 'video' | 'ai' | 'system';
    tag_ids?: string[];
    image?: { url: string; file_key?: string };
    video?: { url?: string; file_key: string; thumbnail_url?: string };
    status?: 'sent' | 'received' | 'recalled' | 'removed';
}

export interface EditMessageSocketPayload {
    message_id: string;
    content: string;
}

export interface RecallMessageSocketPayload {
    message_id: string;
}

export interface ToggleReactionSocketPayload {
    message_id: string;
    emoji: string;
}

// --- Conversation Mutation Payloads ---
export interface CreateConversationSocketPayload {
    type: 'group' | 'utu' | 'self';
    name?: string;
    member_ids: string[];
    primary_icon?: string;
}

export interface UpdateConversationSocketPayload {
    conversation_id: string;
    name?: string;
    avatar_url?: string;
    primary_icon?: string;
}

export interface AddMembersSocketPayload {
    conversation_id: string;
    member_ids: string[];
}

export interface RemoveMembersSocketPayload {
    conversation_id: string;
    member_ids: string[];
}

export interface AssignAdminsSocketPayload {
    conversation_id: string;
    admin_ids: string[];
}

export interface LeaveGroupSocketPayload {
    conversation_id: string;
}

export interface BlockConversationSocketPayload {
    conversation_id: string;
}

export interface DisbandGroupSocketPayload {
    conversation_id: string;
}
