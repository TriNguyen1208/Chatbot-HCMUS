import mongoose, { Document, Schema, model } from "mongoose";

export interface IUser extends Document {
  email: string;
  name: string;
  student_id?: string | undefined;
  phone?: string;
  avatarUrl?: string;
  createdAt: Date;
  updatedAt: Date;
}

const UserSchema = new Schema<IUser>(
  {
    email:     { type: String, required: true, unique: true, lowercase: true, trim: true, index: true },
    name:      { type: String, required: true, trim: true },
    student_id: { type: String, trim: true },
    phone:     { type: String, trim: true },
    avatarUrl: { type: String },
  },
  { timestamps: true , collection: "Users"}
);

export const UserModel = model<IUser>("User", UserSchema)