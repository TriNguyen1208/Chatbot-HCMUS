export interface IQueryOptions<T = Record<string, unknown>> {
    select?: string; 
    limit?: number;
    offset?: number;
    orderBy?: { field: Extract<keyof T, string>; ascending?: boolean };
}

export interface IDatabase {
    // Hàm khởi tạo kết nối đến CSDL
    connect(): Promise<void>
    
    // Hàm ngắt kết nối đến CSDL
    disconnect(): Promise<void>
    
    // Hàm kiểm tra trạng thái kết nối
    isConnected(): boolean
    
    // Escape hatch (lối thoát): Trả về đối tượng Client gốc (mongoose hoặc SupabaseClient)
    getClient<TClient = unknown>(): TClient

    // Hàm lấy danh sách bản ghi dựa trên điều kiện (conditions)
    query<T = Record<string, unknown>>(collectionOrTable: string, conditions?: Partial<T>, options?: IQueryOptions<T>): Promise<T[]>
    
    // Hàm lấy một bản ghi duy nhất
    findOne<T = Record<string, unknown>>(collectionOrTable: string, conditions?: Partial<T>, options?: IQueryOptions<T>): Promise<T | null>
    
    // Hàm thêm mới một hoặc nhiều bản ghi (data: có thể truyền thiếu các trường tự sinh như id, created_at)
    insert<T = Record<string, unknown>>(collectionOrTable: string, data: Partial<T> | Partial<T>[]): Promise<T | T[] | null>
    
    // Hàm cập nhật bản ghi dựa trên điều kiện
    update<T = Record<string, unknown>>(collectionOrTable: string, conditions: Partial<T>, data: Partial<T>): Promise<T | T[] | null>
    
    // Hàm xóa bản ghi dựa trên điều kiện
    delete<T = Record<string, unknown>>(collectionOrTable: string, conditions: Partial<T>): Promise<boolean>
}

