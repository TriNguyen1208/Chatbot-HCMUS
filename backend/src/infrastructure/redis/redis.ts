import {Redis} from "ioredis";
import { config } from "#@/config/config.js";

class RedisClient{
    private static _instance: RedisClient;
    private client: Redis
    constructor(){
        this.client = new Redis({
            host: config.redis.host,
            port: config.redis.port,
            password: config.redis.password,
            lazyConnect: true,
        });
        this.client.on("connect", ()=> {
            console.log("Redis connected")
        })
        this.client.on("error", (e)=> {
            console.error("Redis error: ", e)
        })
    }

    async connect(): Promise<void>{
        await this.client.connect()
    }
    
    getClient(): Redis {
        return this.client;
    }
    async get(key: string): Promise<string | null>{
        try{
            console.log(`[Redis] GET: ${key}`);
            return await this.client.get(key)
        }catch(error){
            return null
        }
    }
    
    async mget(keys: string[]): Promise<(string | null)[]>{
        if (keys.length === 0) return [];
        try{
            console.log(`[Redis] MGET: ${keys.join(', ')}`);
            return await this.client.mget(keys);
        }catch(error){
            return keys.map(() => null);
        }
    }
    async set(key: string, value: string, ttlSeconds: number): Promise<void>{
        try{
            console.log(`[Redis] SET: ${key}`);
            await this.client.set(key, value, "EX", ttlSeconds)
        }catch{
            return
        }
    }
    async getJSON<T = any>(key: string): Promise<T | null>{
        try{
            console.log(`[Redis] GET JSON: ${key}`);
            const data = await this.client.get(key)
            return data ? JSON.parse(data) as T : null;
        }catch(error){
            return null
        }
    }
    async setJSON(key: string, value: any, ttlSeconds: number = 3600): Promise<void>{
        try{
            console.log(`[Redis] SET JSON: ${key}`);
            await this.client.set(key, JSON.stringify(value), "EX", ttlSeconds)
        }catch{
            return
        }
    }
    async del(...keys: string[]): Promise<void>{
        try{
            if (keys.length > 0) {
                console.log(`[Redis] DEL: ${keys.join(', ')}`);
                await this.client.del(...keys)
            }
        }catch{
            return
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
        } catch(error) {
            console.error("[Redis] delByPattern error", error);
        }
    }
}

export const redisClient = new RedisClient();