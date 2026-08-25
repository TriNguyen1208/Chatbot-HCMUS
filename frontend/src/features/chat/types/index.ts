export type ConversationType = 'group' | 'utu';

// export interface Conversation {
//     _id?: string; 
//     name?: string; 
//     type: ConversationType; 
//     member_ids: string[]; 
//     members?: User[];
//     admin_ids?: string[]; 
//     admins?: User[];
//     avatar_url?: string; 
//     primary_icon?: string;
//     last_message_id?: string; 
//     last_message?: Message;
//     created_at?: string;
//     is_active?: boolean;
// }

// export interface Message {
//     _id?: string;
//     conversation_id?: string;
//     conversation?: Conversation;
//     sender_id?: string; 
//     sender?: User;
//     content?: string; 
//     type: 'text' | 'file' | 'link' | 'image' | 'video' | 'ai' | 'system'; 
//     status: 'sent' | 'received' | 'recalled' | 'removed'; 
//     image?: { url: string; file_key?: string }; 
//     video?: { url?: string; file_key: string; thumbnail_url?: string };
//     tag_ids?: string[]; 
//     created_at?: string; 
//     updated_at?: string;
//     is_edited?: boolean;
// }

export interface User {
    id: string;
    email: string;
    name: string;
    student_id?: string;
    phone?: string;
    last_active?: Date | string;
    avatar_url?: string;
    created_at?: string;
}

export interface Message {
    _id?: string;
    sender: User;
    conversation: Conversation;
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
        user?: User;
        emoji: string;
    }[];
    created_at?: Date;
    updated_at?: Date;
    is_edited?: boolean;
}

export interface Conversation {
    _id?: string;
    members: User[];
    admins?: User[];
    last_message?: Message | null;
    created_at: Date;
    name?: string;
    avatar_url?: string;
    type: 'group' | 'utu';
    primary_icon: string;
    is_active: boolean;
}