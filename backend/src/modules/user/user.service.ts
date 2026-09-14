import createHttpError from "http-errors";
import type { IUserRepository } from "./user.repository.js";
import type { UpdateProfileDto } from "./user.dto.js";
import type { User } from "./user.entity.js";
import { triggerSync, SyncOperation } from "#@/shared/utils/sync.util.js";
import { UserCache } from "./user.cache.js";

export class UserService {
    constructor(
        private readonly userRepository: IUserRepository,
        private readonly userCache: UserCache
    ) { }

    /**
     * Retrieves a user by their ID.
     * @param userID The ID of the user to fetch.
     * @returns The user object.
     * @throws HttpError 403 if the user does not exist.
     */
    async getByID(userID: string): Promise<User> {
        const user = await this.findByID(userID);
        if (!user) {
            throw createHttpError.Forbidden("User is not existed");
        }
        return user;
    }

    /**
     * Finds a user by ID without throwing (returns null if not found).
     * Checks cache first; falls back to repository and sets cache.
     */
    async findByID(userID: string): Promise<User | null> {
        const cachedUser = await this.userCache.getUser(userID);
        if (cachedUser) return cachedUser;

        const user = await this.userRepository.findByID(userID);
        if (user && user.id) {
            await this.userCache.setUser(user.id.toString(), user);
        }
        return user;
    }

    /**
     * Finds a user by email.
     */
    async findByEmail(email: string): Promise<User | null> {
        const user = await this.userRepository.findByEmail(email);
        if (user && user.id) {
            await this.userCache.setUser(user.id.toString(), user);
        }
        return user;
    }

    /**
     * Creates a new user record, sets cache, and triggers sync.
     */
    async create(payload: any): Promise<User> {
        const user = await this.userRepository.create(payload);
        if (user && user.id) {
            await this.userCache.setUser(user.id.toString(), user);
        }
        triggerSync('users', SyncOperation.CREATE, user);
        return user;
    }

    /**
     * Updates a user's profile information.
     * @param userID The ID of the user to update.
     * @param payload The data to update (name, avatar, etc.).
     * @returns The updated user object.
     * @throws HttpError 403 if the update fails.
     */
    async update(
        userID: string,
        payload: Partial<User> | UpdateProfileDto
    ): Promise<User> {
        const user = await this.userRepository.update(userID, payload);
        if (!user) {
            throw createHttpError.Forbidden("Update user failed");
        }

        // Update single source of truth cache directly
        await this.userCache.setUser(userID, user);

        triggerSync('users', SyncOperation.UPDATE, user);
        return user;
    }

    /**
     * Retrieves a paginated list of all users.
     * @param limit The maximum number of users to retrieve.
     * @param cursorId The ID used as a cursor for pagination.
     * @returns An array of users.
     */
    async getList(limit: number = 20, cursorId?: string): Promise<User[]> {
        return await this.userRepository.getList(limit, cursorId);
    }

    /**
     * Retrieves multiple users by their IDs with presence.
     * Tries to fetch from Redis first, then fetches missing from DB and updates cache.
     * @param ids Array of user IDs.
     * @returns An array of user objects.
     */
    async getBulk(ids: string[]): Promise<User[]> {
        const uniqueIds = Array.from(new Set(ids));
        if (uniqueIds.length === 0) return [];

        const cachedUsers = await this.userCache.getBulkUsers(uniqueIds);
        const foundUsers = cachedUsers.filter((u): u is User => u !== null);
        const foundIds = new Set(foundUsers.map(u => u.id?.toString()));
        const missingIds = uniqueIds.filter(id => !foundIds.has(id));
        let allUsers = [...foundUsers];

        if (missingIds.length > 0) {
            const missingUsers = await this.userRepository.getBulk(missingIds);
            if (missingUsers.length > 0) {
                await this.userCache.setBulkUsers(missingUsers);
                allUsers = [...allUsers, ...missingUsers];
            }
        }

        if (allUsers.length > 0) {
            const userIds = allUsers.map(u => u.id?.toString() || "");
            const presenceData = await this.userCache.getPresences(userIds);
            allUsers = allUsers.map((user, index) => ({
                ...user,
                is_online: presenceData[index] === "online"
            }));
        }

        return allUsers;
    }

    /**
     * Updates a user's presence (last active time).
     * @param userID The ID of the user.
     * @param lastActive The last active timestamp.
     */
    async updatePresence(userID: string, lastActive: Date): Promise<void> {
        const updated = await this.userRepository.update(userID, { last_active: lastActive });
        if (updated) {
            await this.userCache.setUser(userID, updated);
        } else {
            await this.userCache.invalidateUser(userID);
        }
    }

    /**
     * Sets user's presence to online in cache.
     */
    async setPresenceOnline(userID: string): Promise<void> {
        await this.userCache.setPresenceOnline(userID);
    }

    /**
     * Sets user's presence to offline in cache and updates last_active in database and user cache.
     */
    async setPresenceOffline(userID: string, lastActive: Date): Promise<void> {
        await this.userCache.setPresenceOffline(userID, lastActive);
        await this.updatePresence(userID, lastActive);
    }
}