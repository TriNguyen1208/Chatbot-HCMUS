import type { Message } from "#@/modules/message/entities/message.entity.js";
import type { User } from "#@/modules/user/entities/user.entity.js";
import mongoose, { Schema, Document, Types } from "mongoose";
import { syncConversationES } from "../../../infrastructure/rabbitmq/producer.js";
import { SyncOperation } from "../../../infrastructure/rabbitmq/types.js";

export interface Conversation {
    id?: string;
    member_ids: string[] | Types.ObjectId[];
    admin_ids?: string[] | Types.ObjectId[];
    last_message?: Message | null;
    created_at: Date;
    name?: string;
    avatar_url?: string;
    type: 'group' | 'utu';
    primary_icon: string;
    is_active: boolean;
}

export interface ConversationDB extends Omit<Conversation, 'id' | "last_message" > {
    last_message_id?: Types.ObjectId | null;
    watermarks?: {
        user_id: Types.ObjectId | string;
        last_delivered_msg_id?: Types.ObjectId | string | null;
        last_read_msg_id?: Types.ObjectId | string | null;
    }[];
    _id?: Types.ObjectId;
    __v?: number
}


export const ConversationSchema = new Schema<ConversationDB>({
    member_ids: { type: [{ type: Types.ObjectId, ref: 'User' }], required: true },
    admin_ids: { type: [{ type: Types.ObjectId, ref: 'User' }], default: [] },
    last_message_id: { type: Types.ObjectId, default: null, ref: "Message" },
    watermarks: {
        type: [{
            user_id: { type: Types.ObjectId, ref: 'User' },
            last_delivered_msg_id: { type: Types.ObjectId, ref: 'Message', default: null },
            last_read_msg_id: { type: Types.ObjectId, ref: 'Message', default: null },
            _id: false
        }],
        default: []
    },
    created_at: { type: Date, default: Date.now },
    name: { type: String, required: false },
    avatar_url: { type: String, required: false },
    type: { type: String, enum: ['group', 'utu'], required: true },
    primary_icon: { type: String, required: true },
    is_active: { type: Boolean, default: true, required: true }
});

// Xóa các Mongoose hooks vì logic đã được chuyển sang mongoDBAtlas.ts

export const ConversationModel = mongoose.model<ConversationDB>('Conversation', ConversationSchema);
