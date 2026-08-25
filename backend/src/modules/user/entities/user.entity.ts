import mongoose, { Schema, Types } from "mongoose";

export interface User {
    id?: Types.ObjectId | string;
    email: string;
    name: string;
    student_id?: string;
    phone?: string;
    avatar_url?: string;
    last_active?: Date | string;
    created_at?: Date;
    updated_at?: Date;
}

export interface UserDB extends Omit<User, 'id'> {
    _id?: Types.ObjectId | string;
    __v?: number;
}

export const UserSchema = new Schema<User>({
    email: { type: String, required: true, unique: true },
    name: { type: String, required: true },
    student_id: { type: String, required: false },
    phone: { type: String, required: false },
    avatar_url: { type: String, required: false },
    last_active: { type: Date, default: Date.now },
}, {
    timestamps: { createdAt: 'created_at', updatedAt: 'updated_at' }
});

export const UserModel = mongoose.model<User>('User', UserSchema);