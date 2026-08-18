import mongoose, { Schema, Document, Types } from "mongoose";

export interface Conversation {
    _id?: string;
    member_ids: string[];
    admin_ids?: string[];
    last_message_id?: Types.ObjectId | string | null;
    created_at: Date;
    name?: string;
    avatar_url?: string;
    type: 'group' | 'utu';
    primary_icon: string;
    is_active: boolean;
}

export const ConversationSchema = new Schema<Conversation>({
    member_ids: { type: [String], required: true },
    admin_ids: { type: [String], default: [] },
    last_message_id: { type: Types.ObjectId, default: null, ref: "Message" },
    created_at: { type: Date, default: Date.now },
    name: { type: String, required: false },
    avatar_url: { type: String, required: false },
    type: { type: String, enum: ['group', 'utu'], required: true },
    primary_icon: { type: String, required: true },
    is_active: { type: Boolean, default: true, required: true }
});

export const ConversationModel = mongoose.model<Conversation>('Conversation', ConversationSchema);
