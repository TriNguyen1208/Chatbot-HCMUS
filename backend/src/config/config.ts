import dotenv from "dotenv";
import type jwt from "jsonwebtoken";
import { envSchema } from "./env.schema.js";

dotenv.config();
console.log(process.env)
const parsedEnv = envSchema.parse(process.env);

class Config {
    readonly port: number;
    readonly mongoUri: string;
    readonly mongoAtlasUri: string;
    readonly jwt: {
        accessSecret: string;
        refreshSecret: string;
        accessExpires: jwt.SignOptions['expiresIn'];
        refreshExpires: jwt.SignOptions['expiresIn'];
    };
    readonly google: {
        clientId: string;
    };
    readonly allowedDomains: string[];
    readonly corsOrigins: string[];
    readonly rateLimit: {
        windowMs: number;
        limit: number;
        message: string;
    };
    readonly redis: {
        host: string;
        port: number;
        password?: string;
    };
    readonly elasticsearch: {
        node: string;
    };
    readonly cloudflare: {
        bucket_name: string;
        access_key_id: string;
        secret_access_key: string;
        account_id: string;
        public_url: string;
    };

    constructor() {
        this.port = parsedEnv.PORT;
        this.mongoUri = parsedEnv.MONGO_URI;
        this.mongoAtlasUri = parsedEnv.MONGO_ATLAS_URI;
        this.jwt = {
            accessSecret: parsedEnv.JWT_ACCESS_SECRET,
            refreshSecret: parsedEnv.JWT_REFRESH_SECRET,
            accessExpires: parsedEnv.JWT_ACCESS_EXPIRES_IN as jwt.SignOptions['expiresIn'],
            refreshExpires: parsedEnv.JWT_REFRESH_EXPIRES_IN as jwt.SignOptions['expiresIn'],
        };
        this.google = {
            clientId: parsedEnv.GOOGLE_CLIENT_ID,
        };
        this.allowedDomains = [
            "@student.hcmus.edu.vn",
            "@hcmus.edu.vn",
            "@clc.fitus.edu.vn",
            "@fitus.edu.vn",
            "@fit.hcmus.edu.vn",
            "@apcs.fitus.edu.vn",
            "@vp.fitus.edu.vn",
            "@gmail.com" // for testing
        ];
        this.corsOrigins = parsedEnv.CORS_ORIGINS
            ? parsedEnv.CORS_ORIGINS.split(',')
            : [
                "http://localhost:3000",
                "http://localhost:5500",
                "http://127.0.0.1:5500",
                "https://triunitarian-ethelyn-slushier.ngrok-free.dev"
            ];
        this.rateLimit = {
            windowMs: 15 * 60 * 1000,
            limit: parsedEnv.RATE_LIMIT,
            message: "Too many request from this IP, please try again after 15 minutes",
        };
        this.redis = {
            host: parsedEnv.REDIS_HOST,
            port: parsedEnv.REDIS_PORT,
            password: parsedEnv.REDIS_PASSWORD || undefined,
        };
        this.elasticsearch = {
            node: parsedEnv.ELASTICSEARCH_NODE,
        };
        this.cloudflare = {
            bucket_name: parsedEnv.R2_BUCKET_NAME,
            access_key_id: parsedEnv.R2_ACCESS_KEY_ID,
            secret_access_key: parsedEnv.R2_SECRET_ACCESS_KEY,
            account_id: parsedEnv.R2_ACCOUNT_ID,
            public_url: parsedEnv.R2_PUBLIC_URL,
        };
    }
}

export const config = new Config();