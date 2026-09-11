import { z } from "zod";

export const envSchema = z.object({
    PORT: z.string().default("3001").transform((v) => parseInt(v, 10)),
    MONGO_URI: z.string().default("mongodb://localhost:27017"),
    MONGO_ATLAS_URI: z.string().default("mongodb://localhost:27017"),
    JWT_ACCESS_SECRET: z.string().default("your_access_secret"),
    JWT_REFRESH_SECRET: z.string().default("your_refresh_secret"),
    JWT_ACCESS_EXPIRES_IN: z.string().default("15m"),
    JWT_REFRESH_EXPIRES_IN: z.string().default("7d"),
    GOOGLE_CLIENT_ID: z.string().default(""),
    CORS_ORIGINS: z.string().optional(),
    RATE_LIMIT: z.string().default("1000").transform((v) => parseInt(v, 10)),
    REDIS_HOST: z.string().default("localhost"),
    REDIS_PORT: z.string().default("6379").transform((v) => parseInt(v, 10)),
    REDIS_PASSWORD: z.string().default(""),
    ELASTICSEARCH_NODE: z.string().default("http://localhost:9200"),
    R2_BUCKET_NAME: z.string().default(""),
    R2_ACCESS_KEY_ID: z.string().default(""),
    R2_SECRET_ACCESS_KEY: z.string().default(""),
    R2_ACCOUNT_ID: z.string().default(""),
    R2_PUBLIC_URL: z.string().default(""),
});

export type EnvConfig = z.infer<typeof envSchema>;
