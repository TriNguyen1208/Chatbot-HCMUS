import { apiResponse } from "#@/shared/utils/api-response.js";
import { type Request, type Response, type NextFunction } from "express";
import type { UserService } from "../services/user.service.js";
import type { GetByIDParams, UpdateProfileSchema } from "#@/modules/user/user.dto.js"

import { UserMapper } from "../user.dto.js";
export class UserController {
    constructor(
        private readonly userService: UserService,
    ) { }

    getMe = async (req: Request, res: Response, next: NextFunction) => {
        const user_id = req.user!.userID as string
        const user = await this.userService.getByID(user_id);
        return apiResponse.success(res, UserMapper.toUserResponse(user));
    }

    getByID = async (req: Request, res: Response, next: NextFunction) => {
        const { id: userID } = req.params as GetByIDParams["params"];
        const user = await this.userService.getByID(userID);
        return apiResponse.success(res, UserMapper.toUserResponse(user));
    }

    updateMe = async (req: Request, res: Response, next: NextFunction) => {
        const payload = req.body as UpdateProfileSchema["body"]
        const userID = req.user!.userID
        const user = await this.userService.update(userID, payload)
        return apiResponse.success(res, UserMapper.toUserResponse(user))
    }
}