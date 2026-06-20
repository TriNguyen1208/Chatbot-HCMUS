import { tokenHelper } from "#@/helpers/token.helper.js";
import type { JWTPayload } from "#@/types/index.js";
import type { Request, Response, NextFunction } from "express";
import createHttpError from "http-errors";

export class AuthMiddleware{
    //Verify accessToken
    static verifyAccessToken = (req: Request, res: Response, next: NextFunction) => {
        const token = AuthMiddleware.extractBearer(req)
        if(!token){
            throw createHttpError.Unauthorized("Missing access token")
        }
        const user = tokenHelper.verifyAccessToken(token)
        if(!user || !user.userID || !user.email){
            throw createHttpError.Unauthorized("Invalid token")
        }
        req.user = user
        next()
    }
    
    //Verify refreshToken (Kiểm tra xem refreshToken có đầy đủ payload không, có chứa refreshToken hay không)
    static verifyRefreshToken = (req: Request, res: Response, next: NextFunction) => {
        const { refreshToken }: { refreshToken: string } = req.body;
        if(!refreshToken){
            throw createHttpError.Unauthorized("Missing refreshToken")
        }
        const user = tokenHelper.verifyRefreshToken(refreshToken);
        if(!user || !user.userID || !user.email){
            throw createHttpError.Unauthorized("Invalid token")
        }
        req.user = user
        next()
    }
    //Extract Bear
    private static extractBearer(req: Request): string | undefined {
        const authHeader = req.headers.authorization!;
        return authHeader.split(" ")[1];
    }
}