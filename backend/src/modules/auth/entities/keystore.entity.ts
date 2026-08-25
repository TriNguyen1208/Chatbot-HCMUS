import mongoose, { Schema, Types } from "mongoose";

export interface KeyStore {
    id?: string;
    user_id: Types.ObjectId | string;
    refresh_token_hash: string;
    family_id: string;
    parent_id?: string | null;
    is_used: boolean;
    device_info?: {
        user_agent?: string,
        ip?: string
    } | null;
    expires_at: Date;
}

export interface KeyStoreDB extends Omit<KeyStore, 'id'> {
    _id?: Types.ObjectId | string;
    __v?: number;
}

export const KeyStoreSchema = new Schema<KeyStoreDB>({
    user_id: { type: Types.ObjectId, required: true, ref: 'User' },
    refresh_token_hash: { type: String, required: true },
    family_id: { type: String, required: true },
    parent_id: { type: String, default: null },
    is_used: { type: Boolean, default: false },
    device_info: {
        type: {
            user_agent: String,
            ip: String
        },
        default: null
    },
    expires_at: { type: Date, required: true }
});

export const KeyStoreModel = mongoose.model<KeyStoreDB>('KeyStore', KeyStoreSchema);