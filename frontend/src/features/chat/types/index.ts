import { User } from "../api/user.api";

export type ConversationType = 'group' | 'utu';

export interface Conversation {
    _id?: string; 
    name?: string; 
    type: ConversationType; 
    member_ids: string[]; 
    members?: User[];
    admin_ids?: string[]; 
    admins?: User[];
    avatar_url?: string; 
    primary_icon?: string;
    last_message_id?: string; 
    last_message?: Message;
    created_at?: string;
    is_active?: boolean;
}

export interface Message {
    _id?: string;
    conversation_id?: string;
    conversation?: Conversation;
    sender_id?: string; 
    sender?: User;
    content?: string; 
    type: 'text' | 'file' | 'link' | 'image' | 'video' | 'ai' | 'system'; 
    status: 'sent' | 'received' | 'recalled' | 'removed'; 
    image?: { url: string; file_key?: string }; 
    video?: { url?: string; file_key: string; thumbnail_url?: string };
    tag_ids?: string[]; 
    created_at?: string; 
    updated_at?: string;
    is_edited?: boolean;
}
