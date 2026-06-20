import { config } from "#@/config/index.js";
import type { JWTPayload, TokenPair } from "#@/types/index.js";
import jwt from "jsonwebtoken";

class TokenHelper{
    verifyAccessToken = (token: string) : JWTPayload => {
        return jwt.verify(token, config.jwt.accessSecret) as JWTPayload
    }
    verifyRefreshToken = (token: string) : JWTPayload => {
        return jwt.verify(token, config.jwt.refreshSecret) as JWTPayload
    }
    createAccessToken(userID: string, email: string): string {
        const payload: JWTPayload = { userID, email };
        return jwt.sign(payload, config.jwt.accessSecret, {
            expiresIn: config.jwt.accessExpires as number,
        });
    }
    createRefreshToken(userID: string, email: string): string {
        const payload: JWTPayload = { userID, email };
        return jwt.sign(payload, config.jwt.refreshSecret, {
            expiresIn: config.jwt.refreshExpires as number,
        });
    }
    createPairToken = ({id, email}: {id: string, email: string}): TokenPair => {
        return {
            accessToken: this.createAccessToken(id, email),
            refreshToken: this.createRefreshToken(id, email)
        }
    }
}

export const tokenHelper = new TokenHelper()