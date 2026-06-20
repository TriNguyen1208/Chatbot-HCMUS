import mongoose, {Document, model, Schema, Types} from "mongoose";

export interface IKeyStore extends Document{
    user_id: Types.ObjectId,
    refresh_token_hash: string,
    family_id: string,
    parent_id?: Types.ObjectId | null,
    is_used: boolean | undefined,
    device_info?: {
        user_agent?: string | undefined,
        ip?: string | undefined
    } | undefined,
    expires_at: Date
}

const keyStoreSchema = new Schema<IKeyStore>({
    user_id: {
        type: Types.ObjectId,
        required: true,
        ref: "User",
        index: true
    },

    refresh_token_hash: {
        type: String,
        required: true,
        unique: true
    },
    family_id: {
        type: String,
        required: true,
        unique: true
    },
    parent_id: {
        type: Types.ObjectId,
        default: null 
    },
    is_used: {
        type: Boolean,
        default: false
    },
    device_info: {
        user_agent: {
            type: String
        },
        ip: {
            type: String
        }
    },
    expires_at: { type: Date, required: true },
}, {
    timestamps: true,
    collection: "KeyStores"
})

//Tự động xoá document sau expires_at lớn hơn 30 ngày
keyStoreSchema.index(
    { expires_at: 1 },
    { expireAfterSeconds: 0}
)
export const keyStoreModel = model<IKeyStore>("KeyStore", keyStoreSchema);
