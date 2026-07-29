import { type IDatabase, type IQueryOptions } from "./database.interface.js";
// Dùng FilterQuery (tên chuẩn của thư viện mongodb) để type cho điều kiện query
import mongoose, { type ConnectOptions, type QueryFilter } from "mongoose";
import { config } from "#@/config/config.js";
/**
 * Lớp triển khai (Implementation) của IDatabase dành cho MongoDB Atlas
 */
export class MongoDBAtlas implements IDatabase {
    private connected = false;
    // Hàm kết nối tới cụm MongoDB Atlas
    async connect(): Promise<void> {
        if(this.connected){
            return;
        }
        // Lấy chuỗi kết nối từ biến môi trường
        const uri: string = config.mongoAtlasUri || "";
        
        // Cấu hình các tùy chọn cho kết nối (Connection pool size, timeout)
        const clientOptions: ConnectOptions = {
            maxPoolSize: 10,
            serverSelectionTimeoutMS: 5000,
        };
        
        // Cố gắng mở kết nối bằng mongoose
        try {
            await mongoose.connect(uri, clientOptions);
            this.connected = true;
            console.log("MongoDB connected")
            mongoose.connection.on("disconnected", () => {
                this.connected = false;
                console.warn("⚠️  MongoDB disconnected");
            })
        } catch(error){
            console.error("Error happen: ", error)
            process.exit(1)
        }
    }
    
    // Hàm ngắt kết nối hoàn toàn khỏi MongoDB
    async disconnect(): Promise<void> {
        this.connected = false;
        await mongoose.disconnect();
    }
    
    // Kiểm tra trạng thái mongoose (1 nghĩa là đã kết nối - CONNECTED)
    isConnected(): boolean {
        return mongoose.connection.readyState === 1;
    }

    // Lấy đối tượng mongoose raw (Escape Hatch) để dùng các tính năng riêng của Mongo (như aggregate)
    getClient<TClient = typeof mongoose>(): TClient {
        return mongoose as unknown as TClient;
    }

    // Hàm query để lấy danh sách documents từ collection
    async query<T = Record<string, unknown>>(collection: string, conditions: Partial<T> = {}, options?: IQueryOptions<T>): Promise<T[]> {
        if (!mongoose.connection.db) throw new Error("Not connected to MongoDB");
        
        // Khởi tạo con trỏ cursor tìm kiếm dựa trên điều kiện
        let cursor = mongoose.connection.db.collection(collection).find(conditions);
        
        // Xử lý Projection (chỉ select các field mong muốn, bỏ bớt dữ liệu rác)
        if (options?.select && options.select !== '*') {
            const projection: Record<string, 1 | 0> = {};
            options.select.split(/[ ,]+/).forEach(field => {
                if (field) projection[field] = 1;
            });
            cursor = cursor.project(projection);
        }
        
        // Xử lý sắp xếp (1 là tăng dần, -1 là giảm dần)
        if (options?.orderBy) {
            cursor = cursor.sort({ [options.orderBy.field]: options.orderBy.ascending === false ? -1 : 1 });
        }
        
        // Xử lý bỏ qua bản ghi (Skip / Offset)
        if (options?.offset) {
            cursor = cursor.skip(options.offset);
        }
        
        // Giới hạn số lượng trả về
        if (options?.limit) {
            cursor = cursor.limit(options.limit);
        }

        // Thực thi cursor và chuyển thành mảng
        const result = await cursor.toArray();
        return result as unknown as T[];
    }

    // Hàm lấy 1 document duy nhất
    async findOne<T = Record<string, unknown>>(collection: string, conditions: Partial<T> = {}, options?: IQueryOptions<T>): Promise<T | null> {
        const res = await this.query<T>(collection, conditions, { ...options, limit: 1 });
        if(!res || res.length == 0){
            return null;
        }
        return res[0] ?? null;
    }

    // Hàm chèn thêm data vào collection
    async insert<T = Record<string, unknown>>(collection: string, data: Partial<T> | Partial<T>[]): Promise<T | T[] | null> {
        if (!mongoose.connection.db) throw new Error("Not connected to MongoDB");
        
        if (Array.isArray(data)) {
            const result = await mongoose.connection.db.collection(collection).insertMany(data as unknown as any[]);
            return Object.values(result.insertedIds).map((id, index) => ({ _id: id, ...data[index] })) as unknown as T[];
        } else {
            const result = await mongoose.connection.db.collection(collection).insertOne(data as unknown as any);
            return result.acknowledged ? ({ _id: result.insertedId, ...data } as unknown as T) : null;
        }
    }

    // Hàm cập nhật documents (sử dụng toán tử $set)
    async update<T = Record<string, unknown>>(collection: string, conditions: Partial<T>, data: Partial<T>): Promise<T | T[] | null> {
        if (!mongoose.connection.db) throw new Error("Not connected to MongoDB");
        
        const result = await mongoose.connection.db.collection(collection).updateMany(
            conditions,
            { $set: data } 
        );
        return result.acknowledged ? ({ updatedCount: result.modifiedCount } as unknown as T) : null;
    }
    
    // Hàm xóa documents
    async delete<T = Record<string, unknown>>(collection: string, conditions: Partial<T>): Promise<boolean> {
        if (!mongoose.connection.db) throw new Error("Not connected to MongoDB");
        
        const result = await mongoose.connection.db.collection(collection).deleteMany(conditions);
        
        return result.acknowledged;
    }
}

export const mongoDB = new MongoDBAtlas()