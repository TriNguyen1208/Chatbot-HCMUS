import dotenv from "dotenv"
import jwt from "jsonwebtoken";

dotenv.config()

class Config {
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
    readonly corsOrigins: string[];
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

    readonly cloudflare: {
        bucket_name: string,
        access_key_id: string,
        secret_access_key: string,
        account_id: string,
        public_url: string,
        customer_id: string,
        api_key: string,
        webhook_secret: string
    }

    constructor() {
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
            refreshExpires: (process.env.JWT_REFRESH_EXPIRES_IN || "7d") as jwt.SignOptions['expiresIn'],
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
            "@vp.fitus.edu.vn",
            "@gmail.com" //for testing
        ] as string[]
        this.corsOrigins = process.env.CORS_ORIGINS ? process.env.CORS_ORIGINS.split(',') : [
            "http://localhost:3000",
            "http://localhost:5500",
            "http://127.0.0.1:5500",
            "https://triunitarian-ethelyn-slushier.ngrok-free.dev"
        ];
        this.rateLimit = {
            windowMs: 15 * 60 * 1000,
            limit: parseInt(process.env.RATE_LIMIT || "1000"),
            message: "Too many request from this IP, please try again after 15 minutes",
        }
        this.redis = {
            host: process.env.REDIS_HOST || "localhost",
            port: parseInt(process.env.REDIS_PORT ?? "6379"),
            password: process.env.REDIS_PASSWORD || ""
        }
        this.cloudflare = {
            bucket_name: process.env.R2_BUCKET_NAME as string,
            access_key_id: process.env.R2_ACCESS_KEY_ID as string,
            secret_access_key: process.env.R2_SECRET_ACCESS_KEY as string,
            account_id: process.env.R2_ACCOUNT_ID as string,
            public_url: process.env.R2_PUBLIC_URL as string,
            customer_id: process.env.R2_CUSTOMER_ID as string,
            api_key: process.env.R2_API_KEY as string,
            webhook_secret: process.env.R2_WEBHOOK_SECRET as string
        }
    }
}
export const config = new Config()