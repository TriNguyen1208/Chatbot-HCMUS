import type { UpdateUserProfileDto } from "#@/modules/user/dto/user.dto.js";
import { type IDatabase } from "#@/infrastructure/database/database.interface.js";
import { type User } from "#@/modules/user/entities/user.entity.js";

export interface IUserRepository {
    
    findByEmail(email: string): Promise<User | null>;

    findByID(id: string): Promise<User | null>;

    create({ email, name, avatar_url, student_id }: { email: string; name: string; avatar_url?: string | undefined, student_id?: string | undefined }): Promise<User>;

    update(
        id: string,
        payload: UpdateUserProfileDto
    ): Promise<User | null>;

    getList(limit: number, cursorId?: string): Promise<User[]>;
}

export class UserRepository implements IUserRepository {
    constructor(private readonly db: IDatabase) { }
    /**
     * Finds a user by their email address.
     * @param email The user's email.
     * @returns The user object, or null if not found.
     */
    async findByEmail(email: string): Promise<User | null> {
        const row = await this.db.findOne<User>('users', { email: email.toLowerCase() });
        return row
    }
    /**
     * Finds a user by their unique ID.
     * @param id The user's ID.
     * @returns The user object, or null if not found.
     */
    async findByID(id: string): Promise<User | null> {
        const row = await this.db.findOne<User>('users', { id });
        return row
    }
    /**
     * Creates a new user record in the database.
     * @param param0 Object containing email, name, avatar_url, and student_id.
     * @returns The created user object.
     */
    async create({ email, name, avatar_url, student_id }: { email: string; name: string; avatar_url?: string | undefined, student_id?: string | undefined }): Promise<User> {
        const row = await this.db.insert<User>('users', { email, name, avatar_url, student_id })
        return row as User
    }
     /**
     * Updates an existing user's profile information.
     * @param id The user's ID.
     * @param payload The fields to update.
     * @returns The updated user object, or null if update fails.
     */
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
    /**
     * Retrieves a paginated list of users.
     * @param limit The maximum number of users to fetch.
     * @param cursorId The ID of the last fetched user for pagination.
     * @returns An array of user objects.
     */
    async getList(limit: number = 20, cursorId?: string): Promise<User[]> {
        const client = this.db.getClient<any>();
        let query = client.from('users').select('*').order('id', { ascending: false }).limit(limit);
        
        if (cursorId) {
            query = query.lt('id', cursorId);
        }
        
        const { data, error } = await query;
        if (error) throw new Error(error.message);
        return data as User[];
    }
}