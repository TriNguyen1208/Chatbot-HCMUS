import { studentDirectoryModel, type IStudentDirectory } from "#@/models/student_directory.model.js";

export interface IStudentDirectoryRepository {
    findByEmail(email: string): Promise<IStudentDirectory[]>
}

export class StudentDirectoryRepository implements IStudentDirectoryRepository {
    async findByEmail(email: string): Promise<IStudentDirectory[]> {
        return studentDirectoryModel.find({
            email: email
        }).lean()
    }
}

