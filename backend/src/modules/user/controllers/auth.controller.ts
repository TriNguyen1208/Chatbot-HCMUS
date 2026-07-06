import { apiResponse } from "#@/shared/utils/api-response.js";
import type { AuthService } from "#@/modules/user/services/auth.services.js";
import type { IAuthStrategy } from "#@/modules/user/strategies/auth.strategies.js";
import { type Request, type Response, type NextFunction } from "express";
import createHttpError from "http-errors";
import type { TokenPair, UserProfile } from "#@/shared/types/index.js";
import type { GoogleLoginInput, RefreshTokenInput } from "../user.validator.js";
export class AuthController{
    constructor(
        private readonly authService: AuthService, 
        private readonly authStrategy: IAuthStrategy
    ){
        this.authService = authService;
        this.authStrategy = authStrategy
    }
    googleLogin = async (req: Request, res: Response, next: NextFunction) => {
        const {idToken} = req.body as GoogleLoginInput["body"]

        const result = await this.authService.login(this.authStrategy, idToken);
        //TODO: Ở đây phải có bước lưu refreshToken vào keyStore trong mongo và bước lưu refreshToken vào redis
        await this.authService.saveRefreshToken({
            userID: result.user.id,
            rawRefreshToken: result.tokens.refreshToken,
            device_info: {
                user_agent: req.headers["user-agent"] as string,
                ip: req.ip
            }
        })
        const data = {
            tokens: result.tokens,
            user: result.user
        } as {
            tokens: TokenPair,
            user: Pick<UserProfile, "id" | "email" | "name">
        }
        return apiResponse.success(res, data)
    }
    
    refreshToken = async (req: Request, res: Response, next: NextFunction) => {
        const {refreshToken} = req.body as RefreshTokenInput["body"]
        //TODO: Gửi refreshToken này cho services xử lý
        const tokens = await this.authService.refreshToken({
            rawRefreshToken: refreshToken,
            deviceInfo: {
                user_agent: req.headers["user-agent"] as string,
                ip: req.ip
            },
            user: req.user!
        })
        if(!tokens){
            throw createHttpError.Unauthorized("Vui lòng đăng nhập lại");
        }
        return apiResponse.success(res, tokens)
    }

    logout = async (req: Request, res: Response, next: NextFunction) => {
        //Khi logout thi phai xoa
        const {refreshToken} = req.body as RefreshTokenInput["body"]
        if (!refreshToken) {
            throw createHttpError.BadRequest("refreshToken is required");
        }

        await this.authService.logout(refreshToken)
        return apiResponse.success(res, { message: "Đăng xuất thành công" });
    }
    logoutAll = async (req: Request, res: Response, next: NextFunction) => {
        //Dung user da duoc gan trong req
        const user_id = req.user!.userID
        await this.authService.logoutAll(user_id)
    }
}