import {Redis} from "ioredis";
import { config } from "#@/config/index.js";

class RedisClient{
    private static _instance: RedisClient;
    private client: Redis
    private constructor(){
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

    static getInstance(): RedisClient {
        if(!RedisClient._instance){
            RedisClient._instance = new RedisClient()
        }
        return RedisClient._instance
    }

    async connect(): Promise<void>{
        await this.client.connect()
    }
    async get(key: string): Promise<string | null>{
        try{
            return await this.client.get(key)
        }catch(error){
            return null
        }
    }
    async set(key: string, value: string, ttlSeconds: number): Promise<void>{
        try{
            await this.client.set(key, value, "EX", ttlSeconds)
        }catch{
            return
        }
    }
    async del(...keys: string[]): Promise<void>{
        try{
            await this.client.del(...keys)
        }catch{
            return
        }
    }
}

export const redisClient = RedisClient.getInstance()