import { type IDatabase } from "./database.interface.js";
import mongoose, { type ConnectOptions } from "mongoose";

export class MongoDBAtlas implements IDatabase {
  async connect(): Promise<void> {
    const uri: string = process.env.MONGO_ATLAS_URI || "";
    const clientOptions: ConnectOptions = {
      maxPoolSize: 10,
      serverSelectionTimeoutMS: 5000,
    };
    try {
      await mongoose.connect(uri, clientOptions);
    } finally {
      await mongoose.disconnect();
    }
  }
  async disconnect(): Promise<void> {
    await mongoose.disconnect();
  }
  isConnected(): boolean {
    return mongoose.connection.readyState === 1;
  }
}

export class MongoDBAtlasUser extends MongoDBAtlas {
  
}
