import mongoose from "mongoose";
import { config } from "#@/shared/config/config.js";

class MongoDB{
    private static _instance: MongoDB
    private connected = false
    static getInstance(): MongoDB{
        if(!MongoDB._instance){
            MongoDB._instance = new MongoDB();
        }
        return MongoDB._instance
    }
    async connect(): Promise<void>{
        if(this.connected){
            return;
        }
        try{    
            await mongoose.connect(config.mongoUri)
            this.connected = true
            console.log("MongoDB connected ", config.mongoUri)
            mongoose.connection.on("disconnected", () => {
                this.connected = false;
                console.warn("⚠️  MongoDB disconnected");
            })
        }catch(e){
            console.error("Error happen: ", e)
            process.exit(1)
        }
    }
    async disconnected(): Promise<void>{
        await mongoose.disconnect()
        this.connected = false;
    }
}
export const mongoDB = MongoDB.getInstance();
