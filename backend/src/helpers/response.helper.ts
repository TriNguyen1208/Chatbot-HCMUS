import type { TokenPair, UserProfile } from "#@/types/index.js";
import type { Request, Response, NextFunction } from "express";

export class ResponseHelper{
    success<T>(res: Response, data: T, statusCode = 200): Response{
        return res.status(statusCode).json({
            data: data
        })
    }
    loginSuccess(res: Response, tokens: TokenPair, user: Pick<UserProfile, "id" | "email" | "name">){
        return this.success(res, {tokens, user})
    }
    needsProfile(res: Response, userID: string): Response {
        return this.success(res, {
            needsProfile: true,
            userID,
            message: "Please complete your profile",
        });
    }
}

export const responseHelper = new ResponseHelper()