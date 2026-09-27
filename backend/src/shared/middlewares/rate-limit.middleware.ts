import rateLimit from "express-rate-limit";
import { RedisStore } from "rate-limit-redis";
import { redisClient } from "#@/infrastructure/redis/redis.client.js";
import { config } from "#@/config/config.js";
import type { Request, Response } from "express";

/**
 * Khởi tạo Redis Store dùng chung singleton ioredis client hiện tại của hệ thống
 */
const createRedisStore = (prefix: string) => {
    return new RedisStore({
        sendCommand: (...args: string[]) => {
            const command = args[0];
            if (!command) {
                return Promise.reject(new Error("Redis command must not be empty"));
            }
            const client = redisClient.getClient();
            return client.call(command, ...args.slice(1)) as any;
        },
        prefix: `rl:${prefix}:`,
    });
};

/**
 * Response format chuẩn theo ApiResponse của dự án
 */
const rateLimitErrorHandler = (message: string) => {
    return (_req: Request, res: Response) => {
        res.status(429).json({
            success: false,
            code: 429,
            message,
            data: null,
        });
    };
};

// 1. Global Limiter cho toàn bộ /api (300 requests / 15 phút hoặc theo config)
export const globalRateLimiter = rateLimit({
    windowMs: config.rateLimit.windowMs,
    limit: config.rateLimit.limit,
    standardHeaders: "draft-7",
    legacyHeaders: false,
    store: createRedisStore("global"),
    handler: rateLimitErrorHandler("Bạn đã gửi quá nhiều yêu cầu. Vui lòng thử lại sau 15 phút."),
});

// 2. Strict Limiter cho Auth APIs (Login, Google, Refresh Token) (Tối đa 10 lần / 15 phút)
export const authRateLimiter = rateLimit({
    windowMs: 15 * 60 * 1000,
    limit: 10,
    standardHeaders: "draft-7",
    legacyHeaders: false,
    store: createRedisStore("auth"),
    validate: { keyGeneratorIpFallback: false },
    keyGenerator: (req: Request) => {
        const email = (req.body?.email || req.body?.email_hint || "").toString().toLowerCase().trim();
        return `${req.ip}_${email}`;
    },
    handler: rateLimitErrorHandler("Quá nhiều lần thử xác thực. Vui lòng thử lại sau 15 phút."),
});

// 3. Media Upload Limiter (Tối đa 30 requests / 5 phút per User/IP)
export const mediaRateLimiter = rateLimit({
    windowMs: 5 * 60 * 1000,
    limit: 30,
    standardHeaders: "draft-7",
    legacyHeaders: false,
    store: createRedisStore("media"),
    validate: { keyGeneratorIpFallback: false },
    keyGenerator: (req: any) => {
        return req.user?.id || req.ip;
    },
    handler: rateLimitErrorHandler("Bạn đang tải lên dữ liệu quá nhanh. Vui lòng thử lại sau 5 phút."),
});
