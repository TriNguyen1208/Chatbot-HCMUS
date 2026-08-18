import { type StudentDirectory } from "#@/modules/auth/entities/student_directory.entity.js";
import type { IDatabase } from "#@/infrastructure/database/database.interface.js";

export interface IStudentDirectoryRepository {
    findByEmail(email: string): Promise<StudentDirectory[]>
    findByStudentID(studentId: string): Promise<StudentDirectory[]>
}

export class StudentDirectoryRepository implements IStudentDirectoryRepository {
    constructor(private readonly db: IDatabase) { }

    async findByEmail(email: string): Promise<StudentDirectory[]> {
        return this.db.query("student_directory", { email: email }) as Promise<StudentDirectory[]>
    }

    async findByStudentID(studentId: string): Promise<StudentDirectory[]> {
        return this.db.query("student_directory", { student_id: studentId }) as Promise<StudentDirectory[]>
    }
}

