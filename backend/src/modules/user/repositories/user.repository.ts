import type { UpdateProfileDto } from "#@/modules/user/dto/user.dto.js";
import { type IDatabase } from "#@/infrastructure/database/database.interface.js";
import { type User, type UserDB } from "#@/modules/user/entities/user.entity.js";
import { Types } from "mongoose";

export interface IUserRepository {

    findByEmail(email: string): Promise<User | null>;

    findByID(id: string): Promise<User | null>;
    create(payload: User): Promise<User>
    update(
        id: string,
        payload: Partial<User>
    ): Promise<User | null>;

    getList(limit: number, cursorId?: string): Promise<User[]>;

    getBulk(ids: string[]): Promise<User[]>;
}

export class UserRepository implements IUserRepository {
    constructor(private readonly db: IDatabase) { }

    private mapToDomain(doc: UserDB | null): User | null {
        if (!doc) return null;
        const { _id, __v, ...rest } = doc;
        return {
            id: _id?.toString(),
            ...rest
        } as User;
    }
    /**
     * Finds a user by their email address.
     * @param email The user's email.
     * @returns The user object, or null if not found.
     */
    async findByEmail(email: string): Promise<User | null> {
        const row = await this.db.findOne<UserDB>('users', { email: email.toLowerCase() });
        return this.mapToDomain(row);
    }
    /**
     * Finds a user by their unique ID.
     * @param id The user's ID.
     * @returns The user object, or null if not found.
     */
    async findByID(id: string): Promise<User | null> {
        if (!Types.ObjectId.isValid(id)) return null;
        const row = await this.db.findOne<UserDB>('users', { _id: new Types.ObjectId(id) });
        return this.mapToDomain(row);
    }
    /**
     * Creates a new user record in the database.
     * @param param0 Object containing email, name, avatar_url, and student_id.
     * @returns The created user object.
     */
    async create(payload: User): Promise<User>{
        const row = await this.db.insert<UserDB>('users', payload)
        return this.mapToDomain(row as UserDB | null) as User;
    }
    /**
    * Updates an existing user's profile information.
    * @param id The user's ID.
    * @param payload The fields to update.
    * @returns The updated user object, or null if update fails.
    */
    async update(
        id: string,
        payload: Partial<User>
    ): Promise<User | null> {
        if (!Types.ObjectId.isValid(id)) return null;
        const rows = await this.db.update<UserDB>('users', { _id: new Types.ObjectId(id) }, payload)
        if (!rows || (Array.isArray(rows) && rows.length === 0)) {
            return null;
        }
        const doc = Array.isArray(rows) ? rows[0] : rows;
        return this.mapToDomain(doc as UserDB | null);
    }
    /**
     * Retrieves a paginated list of users.
     * @param limit The maximum number of users to fetch.
     * @param cursorId The ID of the last fetched user for pagination.
     * @returns An array of user objects.
     */
    async getList(limit: number = 20, cursorId?: string): Promise<User[]> {
        let options: any = {
            limit: limit,
            orderBy: { field: '_id', ascending: false }
        };
        let conditions: any = {};

        if (cursorId && Types.ObjectId.isValid(cursorId)) {
            conditions._id = { $lt: new Types.ObjectId(cursorId) };
        }

        const data = await this.db.query<UserDB>('users', conditions, options);
        return data.map(doc => this.mapToDomain(doc as UserDB) as User);
    }

    /**
     * Retrieves multiple users by their IDs.
     * @param ids Array of user IDs.
     * @returns An array of user objects.
     */
    async getBulk(ids: string[]): Promise<User[]> {
        const objectIds = ids.filter(id => Types.ObjectId.isValid(id)).map(id => new Types.ObjectId(id));
        if (objectIds.length === 0) return [];
        
        const data = await this.db.query<UserDB>('users', { _id: { $in: objectIds } } as any, {});
        return data.map(doc => this.mapToDomain(doc as UserDB) as User);
    }
}