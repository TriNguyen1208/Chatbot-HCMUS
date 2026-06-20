import mongoose, {model, Model, Schema, Types} from "mongoose";

export interface IStudentDirectory extends Document{
    student_id: string,
    full_name: string,
    email: string
}

const studentDirectorySchema = new Schema<IStudentDirectory>({
    student_id: {
        type: String,
        required: true,
        unique: true,
        index: true,
    },
    email: {
        type: String,
        required: true,
        index: true,
        trim: true,
        lowercase: true
    },
    full_name: {
        type: String,
        required: true,
        trim: true
    }
},{
    // Nên bật timestamps để biết dữ liệu sinh viên được import vào hệ thống lúc nào
    timestamps: true,
    collection: "StudentDirectories" 
})

export const studentDirectoryModel = model<IStudentDirectory>("StudentDirectory", studentDirectorySchema)