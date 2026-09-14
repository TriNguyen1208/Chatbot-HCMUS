import { Redis } from "ioredis";
import { config } from "#@/config/config.js";

export class RedisClient {
    private client: Redis;

    constructor() {
        this.client = new Redis({
            host: config.redis.host,
            port: config.redis.port,
            password: config.redis.password,
            lazyConnect: true,
        });
        this.client.on("connect", () => {
            console.log("Redis connected");
        });
        this.client.on("error", (e) => {
            console.error("Redis error: ", e);
        });
    }

    async connect(): Promise<void> {
        await this.client.connect();
    }

    getClient(): Redis {
        return this.client;
    }

    async get(key: string): Promise<string | null> {
        try {
            console.log(`[Redis] GET: ${key}`);
            return await this.client.get(key);
        } catch (error) {
            return null;
        }
    }

    async mget(keys: string[]): Promise<(string | null)[]> {
        if (keys.length === 0) return [];
        try {
            console.log(`[Redis] MGET: ${keys.join(', ')}`);
            return await this.client.mget(keys);
        } catch (error) {
            return keys.map(() => null);
        }
    }

    async set(key: string, value: string, ttlSeconds: number): Promise<void> {
        try {
            console.log(`[Redis] SET: ${key}`);
            await this.client.set(key, value, "EX", ttlSeconds);
        } catch {
            return;
        }
    }

    async getJSON<T = any>(key: string): Promise<T | null> {
        try {
            console.log(`[Redis] GET JSON: ${key}`);
            const data = await this.client.get(key);
            return data ? JSON.parse(data) as T : null;
        } catch (error) {
            return null;
        }
    }

    async setJSON(key: string, value: any, ttlSeconds: number = 3600): Promise<void> {
        try {
            console.log(`[Redis] SET JSON: ${key}`);
            await this.client.set(key, JSON.stringify(value), "EX", ttlSeconds);
        } catch {
            return;
        }
    }

    async del(...keys: string[]): Promise<void> {
        try {
            if (keys.length > 0) {
                console.log(`[Redis] DEL: ${keys.join(', ')}`);
                await this.client.del(...keys);
            }
        } catch {
            return;
        }
    }

    async delByPattern(pattern: string): Promise<void> {
        try {
            console.log(`[Redis] DEL PATTERN: ${pattern}`);
            let cursor = '0';
            do {
                const [newCursor, keys] = await this.client.scan(cursor, 'MATCH', pattern, 'COUNT', '100');
                cursor = newCursor;
                if (keys.length > 0) {
                    console.log(`[Redis] DEL (from pattern): ${keys.join(', ')}`);
                    await this.client.del(...keys);
                }
            } while (cursor !== '0');
        } catch (error) {
            console.error("[Redis] delByPattern error", error);
        }
    }

    // Thêm 1 giá trị vào trong set
    async sadd(key: string, ...members: (string | number)[]): Promise<number> {
        if (members.length === 0) return 0;
        try {
            return await this.client.sadd(key, ...members.map(String));
        } catch (error) {
            console.error(`[Redis] SADD error on ${key}:`, error);
            return 0;
        }
    }

    //Kiểm tra 1 giá trị có nằm trong set không
    async sismember(key: string, member: string | number): Promise<boolean> {
        try {
            const result = await this.client.sismember(key, String(member));
            return result === 1;
        } catch (error) {
            console.error(`[Redis] SISMEMBER error on ${key}:`, error);
            return false;
        }
    }

    //Lấy toàn bộ giá trị trong set theo key
    async smembers(key: string): Promise<string[]> {
        try {
            return await this.client.smembers(key);
        } catch (error) {
            console.error(`[Redis] SMEMBERS error on ${key}:`, error);
            return [];
        }
    }

    //Xoá 1 hoặc nhiều phần tử ra khỏi set
    async srem(key: string, ...members: (string | number)[]): Promise<number> {
        if (members.length === 0) return 0;
        try {
            return await this.client.srem(key, ...members.map(String));
        } catch (error) {
            console.error(`[Redis] SREM error on ${key}:`, error);
            return 0;
        }
    }

    // Chèn 1 hoặc nhiều value vào bên trái danh sách (đầu danh sách là left)
    async lpush(key: string, ...values: string[]): Promise<number> {
        if (values.length === 0) return 0;
        try {
            return await this.client.lpush(key, ...values);
        } catch (error) {
            console.error(`[Redis] LPUSH error on ${key}:`, error);
            return 0;
        }
    }

    //Lấy danh sách từ start đến stop
    async lrange(key: string, start: number, stop: number): Promise<string[]> {
        try {
            return await this.client.lrange(key, start, stop);
        } catch (error) {
            console.error(`[Redis] LRANGE error on ${key}:`, error);
            return [];
        }
    }

    //Chỉ lấy phần từ start đến stop, loại bỏ phần còn lại
    async ltrim(key: string, start: number, stop: number): Promise<void> {
        try {
            await this.client.ltrim(key, start, stop);
        } catch (error) {
            console.error(`[Redis] LTRIM error on ${key}:`, error);
        }
    }

    // Set key value trong Hash
    async hset(key: string, data: Record<string, any>): Promise<void> {
        try {
            const stringified: Record<string, string> = {};
            for (const [k, v] of Object.entries(data)) {
                if (v !== undefined && v !== null) {
                    stringified[k] = typeof v === 'object' ? JSON.stringify(v) : String(v);
                }
            }
            if (Object.keys(stringified).length > 0) {
                await this.client.hset(key, stringified);
            }
        } catch (error) {
            console.error(`[Redis] HSET error on ${key}:`, error);
        }
    }

    //Lấy giá trị của 1 field của 1 key
    async hget(key: string, field: string): Promise<string | null> {
        try {
            return await this.client.hget(key, field);
        } catch (error) {
            console.error(`[Redis] HGET error on ${key}:`, error);
            return null;
        }
    }

    //Lấy tất cả value trong key trong Hash
    async hgetall(key: string): Promise<Record<string, string>> {
        try {
            return await this.client.hgetall(key);
        } catch (error) {
            console.error(`[Redis] HGETALL error on ${key}:`, error);
            return {};
        }
    }


    //Lưu thời gian sống của 1 key
    async expire(key: string, ttlSeconds: number): Promise<void> {
        try {
            await this.client.expire(key, ttlSeconds);
        } catch (error) {
            console.error(`[Redis] EXPIRE error on ${key}:`, error);
        }
    }

    //Check exists
    async exists(key: string): Promise<boolean> {
        try {
            return (await this.client.exists(key)) === 1;
        } catch (error) {
            console.error(`[Redis] EXISTS error on ${key}:`, error);
            return false;
        }
    }
}

export const redisClient = new RedisClient();
