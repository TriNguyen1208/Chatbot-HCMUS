export enum SyncOperation {
    CREATE = 'CREATE', 
    UPDATE = 'UPDATE', 
    DELETE = 'DELETE'  
}

export interface SyncPayload<T> {
    operation: SyncOperation; 
    data: T;                  
}
