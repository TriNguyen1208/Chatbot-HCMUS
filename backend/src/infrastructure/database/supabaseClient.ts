import { config } from "#@/config/config.js";
import { type IDatabase, type IQueryOptions } from "./database.interface.js";
import { createClient, SupabaseClient } from "@supabase/supabase-js";


export class SupabaseDatabase implements IDatabase {
    private supabase: SupabaseClient | undefined;
    
    private connected: boolean = true;
    
    private readonly supabaseURL: string;
    
    private readonly supabasePublishableKey: string;

    constructor(url: string, publishableKey: string) {
        this.supabaseURL = url;
        this.supabasePublishableKey = publishableKey;
    }

    // Khởi tạo client kết nối tới Supabase
    async connect(): Promise<void> {
        this.supabase = createClient(this.supabaseURL, this.supabasePublishableKey);
        
        const { error } = await this.supabase.from("users").select("*").limit(1);

        // Bỏ qua lỗi bảng chưa tồn tại (42P01, PGRST116), các lỗi khác sẽ throw
        if (error && error.code !== "42P01" && error.code !== "PGRST116") {
            throw new Error(`Supabase connection failed: ${error.message}`);
        }
        console.log("Supabase connected ")
        this.connected = true;
    }

    async disconnect(): Promise<void> {
        await this.supabase?.removeAllChannels();
    }

    isConnected(): boolean {
        return (this.supabase !== undefined && this.connected);
    }

    getClient<TClient = SupabaseClient>(): TClient {
        return this.supabase as unknown as TClient;
    }

    // Hàm truy vấn danh sách bản ghi
    async query<T = Record<string, unknown>>(table: string, conditions: Partial<T> = {}, options?: IQueryOptions<T>): Promise<T[]> {
        if (!this.supabase) throw new Error("Supabase client not initialized");
        
        let queryBuilder = this.supabase.from(table).select(options?.select || "*");
        
        for (const [key, value] of Object.entries(conditions)) {
            queryBuilder = queryBuilder.eq(key, value as unknown as string);
        }

        if (options?.orderBy) {
            queryBuilder = queryBuilder.order(options.orderBy.field as string, { ascending: options.orderBy.ascending ?? true });
        }
        
        if (options?.limit) {
            queryBuilder = queryBuilder.limit(options.limit);
        }
        
        if (options?.offset) {
            queryBuilder = queryBuilder.range(options.offset, options.offset + (options.limit || 10) - 1);
        }

        const { data, error } = await queryBuilder;
        if (error) throw new Error(`Query error: ${error.message}`);
        
        return data as unknown as T[];
    }

    // Hàm lấy một bản ghi duy nhất
    async findOne<T = Record<string, unknown>>(table: string, conditions: Partial<T> = {}, options?: IQueryOptions<T>): Promise<T | null> {
        const result = await this.query<T>(table, conditions, { ...options, limit: 1 });
        if (!result || result.length === 0) {
            return null; 
        }
        
        return result[0] ?? null;
    }

    // Hàm thêm mới bản ghi (hỗ trợ cả Object lẫn Array Object)
    async insert<T = Record<string, unknown>>(table: string, payload: Partial<T> | Partial<T>[]): Promise<T | T[] | null> {
        if (!this.supabase) throw new Error("Supabase client not initialized");
        
        const { data, error } = await this.supabase
                                .from(table)
                                .insert(payload as any)
                                .select();

        if (error) throw new Error(`Insert error: ${error.message}`);
        
        return (Array.isArray(payload) ? data : data[0]) as unknown as T | T[];
    }

    // Hàm cập nhật bản ghi dựa trên điều kiện
    async update<T = Record<string, unknown>>(table: string, conditions: Partial<T>, payload: Partial<T>): Promise<T | T[] | null> {
        if (!this.supabase) throw new Error("Supabase client not initialized");
        
        let queryBuilder = this.supabase.from(table).update(payload as any).select();
        
        for (const [key, value] of Object.entries(conditions)) {
            queryBuilder = queryBuilder.eq(key, value as unknown as string);
        }

        const { data, error } = await queryBuilder;
        if (error) throw new Error(`Update error: ${error.message}`);
        return data as unknown as T[];
    }

    // Hàm xóa bản ghi
    async delete<T = Record<string, unknown>>(table: string, conditions: Partial<T>): Promise<boolean> {
        if (!this.supabase) throw new Error("Supabase client not initialized");
        
        let queryBuilder = this.supabase.from(table).delete();
        
        for (const [key, value] of Object.entries(conditions)) {
            queryBuilder = queryBuilder.eq(key, value as unknown as string);
        }
        
        const { error } = await queryBuilder;
        if (error) throw new Error(`Delete error: ${error.message}`);
        
        return true;
    }
}

export const supabaseDB = new SupabaseDatabase(
    config.supabase.uri,
    config.supabase.publishableKey
);
