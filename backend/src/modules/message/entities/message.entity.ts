import type { Conversation } from "#@/modules/conversation/entities/conversation.entity.js";
import type { User } from "#@/modules/user/entities/user.entity.js";
import mongoose, { Schema, Document, Types } from "mongoose";
// Entity represents the data of a Message in the database
export interface Message {
    id?: string;             // Unique ID of the message
    sender_id?: Types.ObjectId | string;       // Sender's ID (references the User table)
    receiver_id?: Types.ObjectId | string;
    conversation_id: Types.ObjectId | string; // ID of the conversation containing this message
    content?: string;        // Text content (may not be available if sending image/file)
    type: 'text' | 'file' | 'link' | 'image' | 'video' | 'ai' | 'system'; // Message type
    status?: 'sent' | 'received' | 'recalled' | 'removed';     // Status (sent, received...)
    image?: {
        url: string;
        file_key?: string;
    };
    video?: {
        url?: string;
        file_key: string;
        thumbnail_url?: string;
    };
    tag_ids?: string[];      // List of tags attached to the message
    created_at?: Date;       // Creation time
    updated_at?: Date;       // Last edit time
    is_edited?: boolean;     // Whether the message has been edited
    reactions?: {
        user_id: Types.ObjectId | string;
        emoji: string;
    }[];                     // List of reactions on this message
}

export interface MessageDB extends Omit<Message, 'id' | 'receiver_id'> {
    _id?: Types.ObjectId;
    __v?: number;
}

export const MessageSchema = new Schema<MessageDB>({
    sender_id: { type: Types.ObjectId, ref: 'User' },
    conversation_id: { type: Types.ObjectId, required: true, ref: 'Conversation' },
    content: { type: String, required: false },
    type: { type: String, enum: ['text', 'file', 'link', 'image', 'video', 'ai', 'system'], required: true },
    status: { type: String, enum: ['sent', 'received', 'recalled', 'removed'], default: 'sent' },
    image: {
        url: { type: String, required: false },
        file_key: { type: String, required: false }
    },
    video: {
        url: { type: String, required: false },
        file_key: { type: String, required: false },
        thumbnail_url: { type: String, required: false }
    },
    tag_ids: { type: [String], default: [] },
    created_at: { type: Date, default: Date.now },
    updated_at: { type: Date, required: false },
    is_edited: { type: Boolean, default: false },
    reactions: {
        type: [
            {
                user_id: { type: Types.ObjectId, required: true, ref: 'User' },
                emoji: { type: String, required: true }
            }
        ],
        default: []
    }
});

export const MessageModel = mongoose.model<MessageDB>('Message', MessageSchema);
