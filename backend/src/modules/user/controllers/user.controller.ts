import { apiResponse } from "#@/shared/utils/api-response.js";
import { type Request, type Response, type NextFunction } from "express";
import type { UserService } from "#@/modules/user/services/user.service.js";
import type { GetListQueryDto, GetByIDParamsDto, UpdateProfileDto } from "#@/modules/user/dto/user.dto.js"
import { SearchService } from "#@/modules/search/search.service.js";

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
        return apiResponse.success(res, user);
    }

    /**
     * Retrieves a specific user's profile by their ID.
     * @param req The Express request object containing the user ID in params.
     * @param res The Express response object.
     * @param next The Express next middleware function.
     */
    getByID = async (req: Request, res: Response, next: NextFunction) => {
        const { id: userID } = req.params as GetByIDParamsDto;
        const user = await this.userService.getByID(userID);
        return apiResponse.success(res, user);
    }

    /**
     * Updates the profile of the currently authenticated user.
     * @param req The Express request object containing update payload in body.
     * @param res The Express response object.
     * @param next The Express next middleware function.
     */
    updateMe = async (req: Request, res: Response, next: NextFunction) => {
        const payload = req.body as UpdateProfileDto
        const userID = req.user!.userID
        const user = await this.userService.update(userID, payload)
        return apiResponse.success(res, user)
    }

    /**
     * Retrieves a paginated list of all users.
     * @param req The Express request object containing pagination queries.
     * @param res The Express response object.
     * @param next The Express next middleware function.
     */
    getList = async (req: Request, res: Response, next: NextFunction) => {
        const query = req.query as unknown as GetListQueryDto;
        
        if (query.search) {
            // Thực hiện tìm kiếm qua Elasticsearch
            const searchResults = await SearchService.searchUsers(query.search);
            const userIds = searchResults.map((u: any) => u.id);
            
            // Lấy thông tin chi tiết của user từ DB
            const users = userIds.length > 0 ? await this.userService.getBulk(userIds) : [];
            return apiResponse.success(res, users);
        }

        const users = await this.userService.getList(query.limit, query.cursor_id);
        return apiResponse.success(res, users);
    }

    /**
     * Retrieves multiple users by their IDs in bulk.
     * @param req The Express request object containing user_ids in body.
     * @param res The Express response object.
     * @param next The Express next middleware function.
     */
    getBulk = async (req: Request, res: Response, next: NextFunction) => {
        const { user_ids } = req.body as { user_ids: string[] };
        const users = await this.userService.getBulk(user_ids);
        return apiResponse.success(res, users);
    }
}