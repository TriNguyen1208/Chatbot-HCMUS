import createHttpError from "http-errors";
import type { IUserRepository } from "#@/modules/user/repositories/user.repository.js";
import type { UpdateUserProfileDto } from "#@/modules/user/dto/user.dto.js";
import { redisClient } from "#@/infrastructure/redis/redis.js";

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
    async getByID(userID: string){
        const cacheKey = `user:${userID}`;
        const cachedUser = await redisClient.getJSON(cacheKey);
        if (cachedUser) return cachedUser;

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
    ){
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
    async getList(limit: number, cursorId?: string) {
        const cacheKey = `users:list:${limit}:${cursorId || 'start'}`;
        const cachedList = await redisClient.getJSON(cacheKey);
        if (cachedList) return cachedList;

        const users = await this.userRepository.getList(limit, cursorId);
        await redisClient.setJSON(cacheKey, users, 3600);
        return users;
    }
}