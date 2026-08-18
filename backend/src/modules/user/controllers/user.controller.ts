import { apiResponse } from "#@/shared/utils/api-response.js";
import { type Request, type Response, type NextFunction } from "express";
import type { UserService } from "#@/modules/user/services/user.service.js";
import type { GetByIDParams, UpdateProfileSchema, GetListQueryDto } from "#@/modules/user/dto/user.dto.js"

import { UserMapper } from "#@/modules/user/dto/user.dto.js"
export class UserController {
    constructor(
        private readonly userService: UserService,
    ) { }

    /**
     * Retrieves the profile of the currently authenticated user.
     * @param req The Express request object containing the user's token payload.
     * @param res The Express response object.
     * @param next The Express next middleware function.
     */
    getMe = async (req: Request, res: Response, next: NextFunction) => {
        const user_id = req.user!.userID as string
        const user = await this.userService.getByID(user_id);
        return apiResponse.success(res, UserMapper.toUserResponse(user));
    }

    /**
     * Retrieves a specific user's profile by their ID.
     * @param req The Express request object containing the user ID in params.
     * @param res The Express response object.
     * @param next The Express next middleware function.
     */
    getByID = async (req: Request, res: Response, next: NextFunction) => {
        const { id: userID } = req.params as GetByIDParams["params"];
        const user = await this.userService.getByID(userID);
        return apiResponse.success(res, UserMapper.toUserResponse(user));
    }

    /**
     * Updates the profile of the currently authenticated user.
     * @param req The Express request object containing update payload in body.
     * @param res The Express response object.
     * @param next The Express next middleware function.
     */
    updateMe = async (req: Request, res: Response, next: NextFunction) => {
        const payload = req.body as UpdateProfileSchema["body"]
        const userID = req.user!.userID
        const user = await this.userService.update(userID, payload)
        return apiResponse.success(res, UserMapper.toUserResponse(user))
    }

    /**
     * Retrieves a paginated list of all users.
     * @param req The Express request object containing pagination queries.
     * @param res The Express response object.
     * @param next The Express next middleware function.
     */
    getList = async (req: Request, res: Response, next: NextFunction) => {
        const query = req.query as unknown as GetListQueryDto;
        const users = await this.userService.getList(query.limit, query.cursor_id);
        const userResponses = users.map(u => UserMapper.toUserResponse(u));
        return apiResponse.success(res, userResponses);
    }
}