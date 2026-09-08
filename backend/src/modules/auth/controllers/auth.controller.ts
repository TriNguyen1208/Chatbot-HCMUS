import { apiResponse } from "#@/shared/utils/api-response.js";
import type { AuthService } from "#@/modules/auth/services/auth.service.js";
import type { IAuthStrategy } from "#@/modules/auth/strategies/auth.strategy.js";
import { type Request, type Response, type NextFunction } from "express";
import createHttpError from "http-errors";
import type { GoogleLoginInput } from "#@/modules/auth/dto/auth.dto.js";
import { config } from "#@/config/config.js";
import { parseDurationMs } from "#@/shared/utils/time.utils.js";
import { clearCookie, setCookie } from "#@/shared/utils/cookie.js";
import type { KeystoreService } from "../services/keystore.service.js";
export class AuthController {
    constructor(
        private readonly authService: AuthService,
        private readonly authStrategy: IAuthStrategy,
        private readonly keystoreService: KeystoreService
    ) { }

    /**
     * Authenticates a user using a Google ID token.
     * @param req The Express request object containing the Google idToken in the body.
     * @param res The Express response object used to set token cookies and return user info.
     * @param next The Express next middleware function.
     */
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

        await this.keystoreService.saveRefreshToken({
            userID: result.user.id,
            rawRefreshToken: result.tokens.refreshToken,
            device_info: {
                user_agent: req.headers["user-agent"] as string,
                ip: req.ip
            }
        })
        return apiResponse.success(res, result.user)
    }

    /**
     * Refreshes the authentication tokens using a valid refresh token.
     * Issues a new accessToken and refreshToken while maintaining the original session duration.
     * @param req The Express request object containing the refreshToken in cookies.
     * @param res The Express response object used to set new token cookies (new access and refresh token).
     * @param next The Express next middleware function.
     */
    refreshToken = async (req: Request, res: Response, next: NextFunction) => {
        const refreshToken = req.cookies.refreshToken
        const result = await this.keystoreService.refreshToken({
            rawRefreshToken: refreshToken,
            deviceInfo: {
                user_agent: req.headers["user-agent"] as string,
                ip: req.ip
            },
            user: req.user!
        })
        if (!result.tokens) {
            throw createHttpError.Unauthorized("Please log in again");
        }
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
        return apiResponse.success(res)
    }
    /**
     * Logs out the current user session by removing the refresh token from the database (mark isUsed = true)
     * @param req The Express request object.
     * @param res The Express response object used to clear token cookies.
     * @param next The Express next middleware function.
     */
    logout = async (req: Request, res: Response, next: NextFunction) => {
        const refreshToken = req.cookies.refreshToken
        if (!refreshToken) {
            throw createHttpError.BadRequest("refreshToken is required");
        }
        clearCookie(res, "accessToken")
        clearCookie(res, "refreshToken", "/api/auth")
        await this.authService.logout(refreshToken)
        return apiResponse.success(res, null, { message: "Successfully logged out" });
    }

    /**
     * Logs out the user from all devices by removing all associated refresh tokens in the hierarchy.
     * @param req The Express request object.
     * @param res The Express response object used to clear token cookies.
     * @param next The Express next middleware function.
     */
    logoutAll = async (req: Request, res: Response, next: NextFunction) => {
        const user_id = req.user!.userID
        clearCookie(res, "accessToken")
        clearCookie(res, "refreshToken", "/api/auth")
        await this.authService.logoutAll(user_id)
        return apiResponse.success(res, null, { message: "Successfully logged out all devices" });
    }
}