import mongoose, { Schema, Types } from "mongoose";

export interface User {
    id?: Types.ObjectId | string;
    email: string;
    name: string;
    student_id?: string;
    role?: string;
    phone?: string;
    avatar_url?: string;
    last_active?: Date | string;
    is_online?: boolean;
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
    role: { type: String, default: "Khách" },
    phone: { type: String, required: false },
    avatar_url: { type: String, required: false },
    last_active: { type: Date, default: Date.now },
}, {
    timestamps: { createdAt: 'created_at', updatedAt: 'updated_at' }
});

// Chỉ giữ unique index trên email (đã khai báo trong Schema field) phục vụ đăng nhập.
// Loại bỏ các index không cần thiết (như student_id) để tiết kiệm dung lượng lưu trữ.

export const UserModel = mongoose.model<User>('User', UserSchema);