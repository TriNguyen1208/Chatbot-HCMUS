export interface IQueryOptions<T = Record<string, unknown>> {
    select?: string; 
    limit?: number;
    offset?: number;
    orderBy?: { field: Extract<keyof T, string>; ascending?: boolean };
    populate?: any;
    arrayFilters?: any[];
}

export interface IDatabase {
    connect(): Promise<void>
    
    disconnect(): Promise<void>
    
    isConnected(): boolean
    
    getClient<TClient = unknown>(): TClient

    query<T = Record<string, unknown>>(collectionOrTable: string, conditions?: Partial<T>, options?: IQueryOptions<T>): Promise<T[]>
    
    findOne<T = Record<string, unknown>>(collectionOrTable: string, conditions?: Partial<T>, options?: IQueryOptions<T>): Promise<T | null>
    
    insert<T = Record<string, unknown>>(collectionOrTable: string, data: Partial<T> | Partial<T>[], options?: IQueryOptions<T>): Promise<T | T[] | null>
    
    update<T = Record<string, unknown>>(collectionOrTable: string, conditions: Partial<T>, data: Partial<T> | Record<string, any>, options?: IQueryOptions<T>): Promise<T | T[] | null>
    
    delete<T = Record<string, unknown>>(collectionOrTable: string, conditions: Partial<T>): Promise<boolean>

    findIn<T = Record<string, unknown>>(collectionOrTable: string, column: string, values: any[], options?: IQueryOptions<T>): Promise<T[]>
}

