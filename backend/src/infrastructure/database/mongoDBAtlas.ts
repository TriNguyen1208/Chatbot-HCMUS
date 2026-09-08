import { type IDatabase, type IQueryOptions } from "./database.interface.js";
import mongoose, { type ConnectOptions } from "mongoose";
import { config } from "#@/config/config.js";
import { ConversationModel } from "#@/modules/conversation/entities/conversation.entity.js";
import { MessageModel } from "#@/modules/message/entities/message.entity.js";
import { UserModel } from "#@/modules/user/entities/user.entity.js";
import { KeyStoreModel } from "#@/modules/auth/entities/keystore.entity.js";


export class MongoDBAtlas implements IDatabase {
    private connected = false;
    async connect(): Promise<void> {
        if(this.connected){
            return;
        }
        const uri: string = config.mongoAtlasUri || "";
        
        const clientOptions: ConnectOptions = {
            maxPoolSize: 10,
            serverSelectionTimeoutMS: 5000,
        };
        
        try {
            await mongoose.connect(uri, clientOptions);
            this.connected = true;
            console.log("MongoDB connected")
            mongoose.connection.on("disconnected", () => {
                this.connected = false;
                console.warn("⚠️  MongoDB disconnected");
            })

            try {
                await Promise.all([
                    ConversationModel.createCollection(),
                    MessageModel.createCollection(),
                    UserModel.createCollection(),
                    KeyStoreModel.createCollection(),
                ]);
                console.log("MongoDB collections ensured for all models");
            } catch (err: any) {
                if (err.code !== 48) {
                    console.error("Error creating collections: ", err);
                }
            }
        } catch(error){
            console.error("Error happen: ", error)
            process.exit(1)
        }
    }
    
    async disconnect(): Promise<void> {
        this.connected = false;
        await mongoose.disconnect();
    }
    
    isConnected(): boolean {
        return mongoose.connection.readyState === 1;
    }

    getClient<TClient = typeof mongoose>(): TClient {
        return mongoose as unknown as TClient;
    }

    private getMongooseModel(collection: string) {
        for (const name in mongoose.models) {
            if (mongoose.models[name]?.collection?.collectionName === collection) {
                return mongoose.models[name];
            }
        }
        return null;
    }

    async query<T = Record<string, unknown>>(collection: string, conditions: Partial<T> = {}, options?: IQueryOptions<T>): Promise<T[]> {
        if (!mongoose.connection.db) throw new Error("Not connected to MongoDB");
        
        const Model = this.getMongooseModel(collection);
        if (!Model) {
            throw new Error(`Mongoose Model for collection '${collection}' not found.`);
        }
        let mQuery = Model.find(conditions as any);
        if (options?.select && options.select !== '*') {
            // Chuyển chuỗi select (vd: "name, age") thành chuỗi phân cách bởi khoảng trắng cho Mongoose ("name age")
            mQuery = mQuery.select(options.select.split(/[ ,]+/).join(' '));
        }
        
        if (options?.orderBy) {
            mQuery = mQuery.sort({ [options.orderBy.field]: options.orderBy.ascending === false ? -1 : 1 } as any);
        }
        
        if (options?.offset) {
            mQuery = mQuery.skip(options.offset);
        }
        
        if (options?.limit) {
            mQuery = mQuery.limit(options.limit);
        }
        
        if (options?.populate) {
            if (Array.isArray(options.populate)) {
                options.populate.forEach(p => mQuery = mQuery.populate(p));
            } else {
                mQuery = mQuery.populate(options.populate);
            }
        }
        // Dùng .lean() để tối ưu tốc độ, kết quả trả về giống hệt Native Driver
        return await mQuery.lean().exec() as unknown as T[];
    }

    async findOne<T = Record<string, unknown>>(collection: string, conditions: Partial<T> = {}, options?: IQueryOptions<T>): Promise<T | null> {
        const res = await this.query<T>(collection, conditions, { ...options, limit: 1 });
        if(!res || res.length == 0){
            return null;
        }
        return res[0] ?? null;
    }

    async insert<T = Record<string, unknown>>(collection: string, data: Partial<T> | Partial<T>[], options?: IQueryOptions<T>): Promise<T | T[] | null> {
        if (!mongoose.connection.db) throw new Error("Not connected to MongoDB");
        
        const Model = this.getMongooseModel(collection);
        if (!Model) {
            throw new Error(`Mongoose Model for collection '${collection}' not found.`);
        }

        let result: any;
        if (Array.isArray(data)) {
            result = await Model.insertMany(data);
        } else {
            result = await Model.create(data);
        }

        if (options?.populate) {
            await Model.populate(result, options.populate);
        }

        const finalResult = Array.isArray(result) 
            ? result.map(doc => doc.toObject()) as unknown as T[] 
            : result.toObject() as unknown as T;

        return finalResult;
    }

    async update<T = Record<string, unknown>>(collection: string, conditions: Partial<T>, data: Partial<T> | Record<string, any>, options?: IQueryOptions<T>): Promise<T | T[] | null> {
        if (!mongoose.connection.db) throw new Error("Not connected to MongoDB");
        
        // Kiểm tra xem data có chứa MongoDB operators (ví dụ: $set, $addToSet, $push) hay không
        const isOperatorQuery = Object.keys(data).some(key => key.startsWith('$'));
        const updateQuery = isOperatorQuery ? data : { $set: data };

        // Use findOneAndUpdate to avoid a second query, returning the updated document
        const updateOptions: any = { returnDocument: 'after' };
        if (options?.arrayFilters) {
            updateOptions.arrayFilters = options.arrayFilters;
        }

        const result = await mongoose.connection.db.collection(collection).findOneAndUpdate(
            conditions,
            updateQuery,
            updateOptions
        );

        if (!result) return null;

        if (options?.populate) {
            const Model = this.getMongooseModel(collection);
            if (Model) {
                await Model.populate(result, options.populate);
            }
        }

        return result as unknown as T;
    }
    
    async delete<T = Record<string, unknown>>(collection: string, conditions: Partial<T>): Promise<boolean> {
        if (!mongoose.connection.db) throw new Error("Not connected to MongoDB");
        
        const result = await mongoose.connection.db.collection(collection).findOneAndDelete(conditions);
        
        if (result) {
            return true;
        }
        
        return false;
    }

    async findIn<T = Record<string, unknown>>(collection: string, column: string, values: any[], options?: IQueryOptions<T>): Promise<T[]> {
        if (!mongoose.connection.db) throw new Error("Not connected to MongoDB");
        
        if (!values || values.length === 0) return [];
        const conditions = { [column]: { $in: values } };

        const Model = this.getMongooseModel(collection);
        if (!Model) {
            throw new Error(`Mongoose Model for collection '${collection}' not found.`);
        }

        let mQuery = Model.find(conditions as any);
        
        if (options?.select && options.select !== '*') {
            mQuery = mQuery.select(options.select.split(/[ ,]+/).join(' '));
        }
        
        if (options?.orderBy) {
            mQuery = mQuery.sort({ [options.orderBy.field]: options.orderBy.ascending === false ? -1 : 1 } as any);
        }
        
        if (options?.offset) {
            mQuery = mQuery.skip(options.offset);
        }
        
        if (options?.limit) {
            mQuery = mQuery.limit(options.limit);
        }
        
        if (options?.populate) {
            if (Array.isArray(options.populate)) {
                options.populate.forEach(p => mQuery = mQuery.populate(p));
            } else {
                mQuery = mQuery.populate(options.populate);
            }
        }

        return await mQuery.lean().exec() as unknown as T[];
    }
}

export const mongoDB = new MongoDBAtlas()