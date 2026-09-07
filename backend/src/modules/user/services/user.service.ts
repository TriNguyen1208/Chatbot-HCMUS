import createHttpError from "http-errors";
import type { IUserRepository } from "#@/modules/user/repositories/user.repository.js";
import type { UpdateProfileDto } from "#@/modules/user/dto/user.dto.js";
import { redisClient } from "#@/infrastructure/redis/redis.js";
import type { User } from "../entities/user.entity.js";
import { triggerSync } from "#@/utils/sync.util.js";
import { SyncOperation } from "#@/infrastructure/rabbitmq/types.js";

export class UserService {
    constructor(
        private readonly userRepository: IUserRepository
    ) { }

    /**
     * Retrieves a user by their ID.
     * @param userID The ID of the user to fetch.
     * @returns The user object.
     * @throws HttpError 403 if the user does not exist.
     */
    async getByID(userID: string): Promise<User>{
        //Get cache first
        const cacheKey = `user:${userID}`;
        const cachedUser = await redisClient.getJSON(cacheKey);
        if (cachedUser) return cachedUser;

        //Cache miss
        const user = await this.userRepository.findByID(userID);
        if(!user){
            throw createHttpError.Forbidden("User is not existed")
        }

        await redisClient.setJSON(cacheKey, user, 3600); // 1 hour TTL
        return user
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
        payload: UpdateProfileDto
    ): Promise<User>{
        //Update database and delete cache (Cache-aside method)
        const user = await this.userRepository.update(userID, payload);
        if(!user){
            throw createHttpError.Forbidden("Update user failed")
        }

        // Invalidate cache
        await redisClient.del(`user:${userID}`);
        await redisClient.delByPattern('users:list:*');

        triggerSync('users', SyncOperation.UPDATE, user);

        return user;
    }
    
    /**
     * Retrieves a paginated list of all users.
     * @param limit The maximum number of users to retrieve.
     * @param cursorId The ID used as a cursor for pagination.
     * @returns An array of users.
     */
    async getList(limit: number, cursorId?: string): Promise<User[]> {
        const users = await this.userRepository.getList(limit, cursorId);
        return users;
    }

    /**
     * Retrieves multiple users by their IDs.
     * Tries to fetch from Redis first, then fetches missing from DB and updates cache.
     * @param ids Array of user IDs.
     * @returns An array of user objects.
     */
    async getBulk(ids: string[]): Promise<User[]> {
        const uniqueIds = Array.from(new Set(ids));
        const cacheKeys = uniqueIds.map(id => `user:${id}`);
        
        let cachedUsers: (User | null)[] = [];
        if (cacheKeys.length > 0) {
            // Redis MGET for JSON is not standard in some libs, so we might need multiple gets, 
            // but for simplicity we will just loop or use mget if available.
            // Using a loop for guaranteed JSON parse safety.
            cachedUsers = await Promise.all(cacheKeys.map(key => redisClient.getJSON(key)));
        }

        const foundUsers = cachedUsers.filter(u => u !== null) as User[];
        const foundIds = new Set(foundUsers.map(u => u.id));
        const missingIds = uniqueIds.filter(id => !foundIds.has(id));
        let allUsers = [...foundUsers];

        if (missingIds.length > 0) {
            const missingUsers = await this.userRepository.getBulk(missingIds);
            
            // Save missing users to cache
            await Promise.all(missingUsers.map(u => 
                redisClient.setJSON(`user:${u.id}`, u, 3600)
            ));

            allUsers = [...allUsers, ...missingUsers];
        }

        if (allUsers.length > 0) {
            const presenceKeys = allUsers.map(u => `presence:${u.id}`);
            const presenceData = await redisClient.mget(presenceKeys);
            
            allUsers = allUsers.map((user, index) => {
                const isOnline = presenceData[index] === "online";
                return {
                    ...user,
                    is_online: isOnline
                };
            });
        }

        return allUsers;
    }

    /**
     * Updates a user's presence (last active time).
     * @param userID The ID of the user.
     * @param lastActive The last active timestamp.
     */
    async updatePresence(userID: string, lastActive: Date): Promise<void> {
        await this.userRepository.update(userID, { last_active: lastActive });
        // Invalidate cache for this user
        await redisClient.del(`user:${userID}`);
    }
}