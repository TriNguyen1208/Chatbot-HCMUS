import { apiResponse } from "#@/shared/utils/api-response.js";
import type { AuthService } from "#@/modules/user/services/auth.service.js";
import type { IAuthStrategy } from "#@/modules/user/strategies/auth.strategy.js";
import { type Request, type Response, type NextFunction } from "express";
import createHttpError from "http-errors";
import type { GoogleLoginInput } from "#@/modules/user/user.dto.js";
import { config } from "#@/config/config.js";
import { parseDurationMs } from "#@/shared/utils/time.utils.js";
import { clearCookie, setCookie } from "#@/shared/utils/cookie.js";
import { UserMapper } from "../user.dto.js";

export class AuthController {
    constructor(
        private readonly authService: AuthService,
        private readonly authStrategy: IAuthStrategy
    ) {
        this.authService = authService;
        this.authStrategy = authStrategy
    }
    googleLogin = async (req: Request, res: Response, next: NextFunction) => {
        const { idToken } = req.body as GoogleLoginInput["body"]
        const result = await this.authService.login(this.authStrategy, idToken);
        setCookie(
            res,
            "accessToken",
            result.tokens.accessToken,
            parseDurationMs(config.jwt.accessExpires as string),
            "/"
        )
        setCookie(
            res,
            "refreshToken",
            result.tokens.refreshToken,
            parseDurationMs(config.jwt.refreshExpires as string),
            "/api/auth"
        )
        await this.authService.saveRefreshToken({
            userID: result.user.id,
            rawRefreshToken: result.tokens.refreshToken,
            device_info: {
                user_agent: req.headers["user-agent"] as string,
                ip: req.ip
            }
        })
        return apiResponse.success(res, UserMapper.toUserResponse(result.user))
    }

    refreshToken = async (req: Request, res: Response, next: NextFunction) => {
        // const { refreshToken } = req.body as RefreshTokenInput["body"]
        const refreshToken = req.cookies.refreshToken
        const result = await this.authService.refreshToken({
            rawRefreshToken: refreshToken,
            deviceInfo: {
                user_agent: req.headers["user-agent"] as string,
                ip: req.ip
            },
            user: req.user!
        })
        if (!result.tokens) {
            throw createHttpError.Unauthorized("Vui lòng đăng nhập lại");
        }
        setCookie(
            res,
            "accessToken",
            result.tokens.accessToken,
            parseDurationMs(config.jwt.accessExpires as string),
            "/"
        )
        const timeRemaining = new Date(result.expires_refresh_token).getTime() - Date.now();
        setCookie(
            res,
            "refreshToken",
            result.tokens.refreshToken,
            timeRemaining,
            "/api/auth"
        )
        return apiResponse.success(res)
    }

    logout = async (req: Request, res: Response, next: NextFunction) => {
        const refreshToken = req.cookies.refreshToken
        if (!refreshToken) {
            throw createHttpError.BadRequest("refreshToken is required");
        }
        clearCookie(res, "accessToken")
        clearCookie(res, "refreshToken", "/api/auth")
        await this.authService.logout(refreshToken)
        return apiResponse.success(res, { message: "Đăng xuất thành công" });
    }
    logoutAll = async (req: Request, res: Response, next: NextFunction) => {
        const user_id = req.user!.userID
        clearCookie(res, "accessToken")
        clearCookie(res, "refreshToken", "/api/auth")
        await this.authService.logoutAll(user_id)
        return apiResponse.success(res, { message: "Đăng xuất tất cả thiết bị thành công" });
    }

    getMe = async (req: Request, res: Response, next: NextFunction) => {
        const user_id = req.user!.userID;
        const user = await this.authService.getMe(user_id);
        return apiResponse.success(res, UserMapper.toUserResponse(user));
    }
}