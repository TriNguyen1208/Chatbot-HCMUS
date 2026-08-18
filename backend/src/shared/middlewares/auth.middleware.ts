import { jwtService } from "#@/shared/utils/jwt-services.js";
import type { JWTPayload } from "#@/shared/types/index.js";
import type { Request, Response, NextFunction } from "express";
import createHttpError from "http-errors";

export class AuthMiddleware {
    //Verify accessToken
    static verifyAccessToken = (req: Request, res: Response, next: NextFunction) => {
        const token = req.cookies.accessToken
        if (!token) {
            throw createHttpError.Unauthorized("Missing access token")
        }
        const user = jwtService.verifyAccessToken(token)
        if (!user || !user.userID || !user.email) {
            throw createHttpError.Unauthorized("Invalid token")
        }
        req.user = user
        next()
    }

    //Verify refreshToken (Check if refreshToken has full payload, contains refreshToken or not)
    static verifyRefreshToken = (req: Request, res: Response, next: NextFunction) => {
        const refreshToken = req.cookies.refreshToken
        if (!refreshToken) {
            throw createHttpError.Unauthorized("Missing refreshToken")
        }
        const user = jwtService.verifyRefreshToken(refreshToken);
        if (!user || !user.userID || !user.email) {
            throw createHttpError.Unauthorized("Invalid token")
        }
        req.user = user
        next()
    }
}