import mongoose from "mongoose";
import { config } from "#@/config/index.js";

class Database{
    private static _instance: Database
    private connected = false
    static getInstance(): Database{
        if(!Database._instance){
            Database._instance = new Database();
        }
        return Database._instance
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
export const database = Database.getInstance();
