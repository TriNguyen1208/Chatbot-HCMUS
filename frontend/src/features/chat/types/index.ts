export interface User {
    id: string;
    _id?: string;
    email: string;
    name: string;
    student_id?: string;
    phone?: string;
    last_active?: Date | string;
    avatar_url?: string;
    is_online?: boolean;
    created_at?: string;
}

export interface Message {
    id?: string;
    sender_id?: string;
    conversation_id?: string;
    content?: string;
    type: 'text' | 'file' | 'link' | 'image' | 'video' | 'ai' | 'system';
    status?: 'sent' | 'received' | 'recalled' | 'removed';
    image?: {
        url: string;
        file_key?: string;
    };
    video?: {
        url?: string;
        file_key: string;
        thumbnail_url?: string;
    };
    tag_ids?: string[];
    reactions?: {
        user_id?: string;
        emoji: string;
    }[];
    created_at?: Date | string;
    updated_at?: Date | string;
    is_edited?: boolean;
    edit_history?: { content: string; updated_at: Date | string }[];
}

export interface Conversation {
    id?: string;
    member_ids?: string[];
    admin_ids?: string[];
    last_message?: Message | null;
    created_at?: Date | string;
    name?: string;
    avatar_url?: string;
    type: 'group' | 'utu';
    primary_icon?: string;
    is_active?: boolean;
    receiver_id?: string;
    watermarks?: {
        user_id: string;
        last_delivered_msg_id?: string | null;
        last_read_msg_id?: string | null;
    }[];
}
