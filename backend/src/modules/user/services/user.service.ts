import createHttpError from "http-errors";
import { UserRepository, type IUserRepository } from "../repositories/user.repository.js";
import type { UpdateUserProfileDto } from "#@/modules/user/user.dto.js";
import type { Types } from "mongoose";

export class UserService{
    constructor(
        private readonly userRepository: IUserRepository
    ){}

    async getByID(userID: string){
        const user = await this.userRepository.findByID(userID);
        if(!user){
            throw createHttpError.Forbidden("User is not existed")
        }
        return user
    }
    
    async update(
        userID: string,
        payload: UpdateUserProfileDto
    ){
        const user = await this.userRepository.update(userID, payload);
        if(!user){
            throw createHttpError.Forbidden("Update user failed")
        }
        return user;
    }
}