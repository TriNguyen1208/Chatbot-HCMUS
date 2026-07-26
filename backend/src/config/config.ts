import dotenv from "dotenv"
import jwt from "jsonwebtoken";

dotenv.config()

class Config{
    private static _instance: Config

    readonly port: number
    readonly mongoUri: string;
    readonly mongoAtlasUri: string;
    readonly supabase: {
        uri: string,
        publishableKey: string,
    }
    readonly jwt: {
        accessSecret: string,
        refreshSecret: string,
        accessExpires: jwt.SignOptions['expiresIn'],
        refreshExpires: jwt.SignOptions['expiresIn']
    }
    readonly google: {
        clientId: string;
    }
    readonly allowedDomains: string[];
    readonly rateLimit: {
        windowMs: number,
        limit: number,
        message: string
    }
    readonly redis: {
        host: string,
        port: number,
        password?: string
    }

    private constructor(){
        this.port = parseInt(process.env.PORT ?? "3001")
        this.mongoUri = process.env.MONGO_URI || "mongodb://localhost:27017"
        this.mongoAtlasUri = process.env.MONGO_ATLAS_URI || "mongodb://localhost:27017";
        this.supabase = {
            uri: process.env.SUPABASE_URL || "",
            publishableKey: process.env.SUPABASE_PUBLISHABLE_KEY || "",
        }
        this.jwt = {
            accessSecret: process.env.JWT_ACCESS_SECRET as string || "your_access_secret",
            refreshSecret: process.env.JWT_REFRESH_SECRET as string || "your_refresh_secret",
            accessExpires: (process.env.JWT_ACCESS_EXPIRES_IN || "15m") as jwt.SignOptions['expiresIn'],
            refreshExpires: (process.env.JWT_REFRESH_SECRET_IN || "7d") as jwt.SignOptions['expiresIn'],
            
        }
        this.google = {
            clientId: process.env.GOOGLE_CLIENT_ID || ""
        }
        this.allowedDomains = [
            "@student.hcmus.edu.vn",
            "@hcmus.edu.vn",
            "@clc.fitus.edu.vn",
            "@fitus.edu.vn",
            "@fit.hcmus.edu.vn",
            "@apcs.fitus.edu.vn",
            "@vp.fitus.edu.vn"
        ] as string[]
        this.rateLimit = {
            windowMs: 15 * 60 * 1000,
            limit: 100,
            message: "Too many request from this IP, please try again after 15 minutes",
        }
        this.redis = {
            host: process.env.REDIS_HOST || "localhost",
            port: parseInt(process.env.REDIS_PORT ?? "6379"),
            password: process.env.REDIS_PASSWORD || ""
        }
    }

    static getInstance(): Config {
        if(!Config._instance){
            Config._instance = new Config()
        }
        return Config._instance
    }
}
export const config = Config.getInstance()