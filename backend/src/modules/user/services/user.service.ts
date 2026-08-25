import createHttpError from "http-errors";
import type { IUserRepository } from "#@/modules/user/repositories/user.repository.js";
import type { UpdateUserProfileDto } from "#@/modules/user/dto/user.dto.js";
import { redisClient } from "#@/infrastructure/redis/redis.js";
import type { User } from "../entities/user.entity.js";

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
        payload: UpdateUserProfileDto
    ): Promise<User>{
        //Update database and delete cache (Cache-aside method)
        const user = await this.userRepository.update(userID, payload);
        if(!user){
            throw createHttpError.Forbidden("Update user failed")
        }

        // Invalidate cache
        await redisClient.del(`user:${userID}`);
        await redisClient.delByPattern('users:list:*');

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
     * Handles a user connecting a new socket.
     * @param userID The user's ID
     * @param socketID The socket's ID
     * @returns boolean true if the user just came online
     */
    async userConnect(userID: string, socketID: string): Promise<boolean> {
        const client = redisClient.getClient();
        await client.sadd(`presence:sockets:${userID}`, socketID);
        await client.set(`presence:heartbeat:${userID}`, "1", "EX", 45); // Heartbeat with TTL

        const count = await client.scard(`presence:sockets:${userID}`);
        if (count === 1) {
            // Cập nhật last_active là thời điểm hiện tại khi online (Tuỳ chọn, nhưng giúp record thời điểm bắt đầu session)
            await this.update(userID, { last_active: new Date().toISOString() });
            return true; 
        }
        return false;
    }

    /**
     * Handles a user disconnecting a socket.
     * @param userID The user's ID
     * @param socketID The socket's ID
     * @returns boolean true if the user just went offline
     */
    async userDisconnect(userID: string, socketID: string): Promise<boolean> {
        const client = redisClient.getClient();
        await client.srem(`presence:sockets:${userID}`, socketID);
        const count = await client.scard(`presence:sockets:${userID}`);
        
        if (count === 0) {
            // Delete heartbeat
            await client.del(`presence:heartbeat:${userID}`);
            // Save last_active
            const lastActive = new Date().toISOString();
            await this.update(userID, { last_active: lastActive });
            return true; // Just went offline
        }
        return false;
    }

    /**
     * Renews the heartbeat TTL for the user.
     * @param userID The user's ID
     */
    async userHeartbeat(userID: string): Promise<void> {
        const client = redisClient.getClient();
        await client.set(`presence:heartbeat:${userID}`, "1", "EX", 45);
    }

    /**
     * Gets the online status of multiple users.
     * @param userIDs Array of user IDs
     * @returns A map of userID to isOnline boolean
     */
    async getUsersStatus(userIDs: string[]): Promise<Record<string, boolean>> {
        if (!userIDs || userIDs.length === 0) return {};
        
        const client = redisClient.getClient();
        const pipeline = client.pipeline();
        
        userIDs.forEach(id => {
            pipeline.exists(`presence:heartbeat:${id}`);
        });
        
        const results = await pipeline.exec();
        const statusMap: Record<string, boolean> = {};
        
        for (let i = 0; i < userIDs.length; i++) {
            const id = userIDs[i] as string;
            // results[i] is [error, result] where result is 1 (exists) or 0 (not exists)
            const isOnline = results && results[i] && !results[i]![0] ? results[i]![1] === 1 : false;
            statusMap[id] = isOnline;
        }
        
        return statusMap;
    }
}