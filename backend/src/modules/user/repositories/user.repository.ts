import type { UpdateUserProfileDto } from "#@/modules/user/user.dto.js";
import { type IDatabase } from "#@/infrastructure/database/database.interface.js";
import { type User } from "../entities/user.entity.js";

export interface IUserRepository {
    findByEmail(email: string): Promise<User | null>
    findByID(id: string): Promise<User | null>
    create({ email, name, avatar_url, student_id }: { email: string; name: string; avatar_url: string | undefined, student_id?: string | undefined }): Promise<User>;
    update(
        id: string,
        payload: UpdateUserProfileDto
    ): Promise<User | null>
}

export class UserRepository implements IUserRepository {
    constructor(private readonly db: IDatabase) { }
    async findByEmail(email: string): Promise<User | null> {
        const row = await this.db.findOne<User>('users', { email: email.toLowerCase() });
        return row
    }
    async findByID(id: string): Promise<User | null> {
        const row = await this.db.findOne<User>('users', { id });
        return row
    }
    async create({ email, name, avatar_url, student_id }: { email: string; name: string; avatar_url: string; student_id?: string | undefined }): Promise<User> {
        const row = await this.db.insert<User>('users', { email, name, avatar_url, student_id })
        return row as User
    }
    async update(
        id: string,
        payload: UpdateUserProfileDto
    ): Promise<User | null> {
        const rows = await this.db.update<User>('users', { id }, payload)
        if (!rows || (Array.isArray(rows) && rows.length === 0)) {
            return null;
        }
        return Array.isArray(rows) ? rows[0] as User : rows as User
    }
}