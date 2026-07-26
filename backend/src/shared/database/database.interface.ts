export interface IDatabase {
  connect(): Promise<void>
  disconnect(): Promise<void>
  isConnected(): boolean
}

export interface IDatabaseUserService {
  findUserNames(): Promise<string[]>
}