import { responseHelper } from "#@/helpers/response.helper.js";
import type { AuthService } from "#@/services/auth.service.js";
import type { IAuthStrategy } from "#@/strategies/auth.strategies.js";
import { type Request, type Response, type NextFunction } from "express";
import createHttpError from "http-errors";

export class AuthController{
    constructor(
        private readonly authService: AuthService, 
        private readonly authStrategy: IAuthStrategy
    ){
        this.authService = authService;
        this.authStrategy = authStrategy
    }
    googleLogin = async (req: Request, res: Response, next: NextFunction) => {
        const {idToken} = req.body as {idToken?: string}
        if(!idToken){
            throw createHttpError.Unauthorized("ID Token is required")
        }
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
        
        return responseHelper.loginSuccess(res, result.tokens, result.user)
    }
    
    refreshToken = async (req: Request, res: Response, next: NextFunction) => {
        const {refreshToken} = req.body as {refreshToken: string}
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
        return responseHelper.success(res, tokens)
    }

    logout = async (req: Request, res: Response, next: NextFunction) => {
        //Khi logout thi phai xoa
        const {refresh_token} = req.body as {refresh_token: string}
        if (!refresh_token) {
            throw createHttpError.BadRequest("refreshToken is required");
        }

        await this.authService.logout(refresh_token)
        return responseHelper.success(res, { message: "Đăng xuất thành công" });
    }
    logoutAll = async (req: Request, res: Response, next: NextFunction) => {
        //Dung user da duoc gan trong req
        const user_id = req.user!.userID
        await this.authService.logoutAll(user_id)
    }
}