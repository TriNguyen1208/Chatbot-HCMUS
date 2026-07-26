import { type IDatabase } from "#@/infrastructure/database/database.interface.js";
import { type KeyStore } from "#@/modules/user/entities/keystore.entity.js";

export interface IKeyStoreRepository {
    create(data: KeyStore): Promise<KeyStore>
    findByHash(hashRefreshToken: string): Promise<KeyStore | null>
    revokeFamily(family_id: string): Promise<void>
    markUsed(id: string): Promise<void>
    markUsedByHash(hash: string): Promise<void>
    revokeAllByUser(user_id: string): Promise<void>
}


export class KeyStoreRepository implements IKeyStoreRepository {
    constructor(private readonly db: IDatabase) { }

    async create(data: KeyStore): Promise<KeyStore> {
        return this.db.insert<KeyStore>("keystores", data) as Promise<KeyStore>
    }
    async findByHash(hashRefreshToken: string): Promise<KeyStore | null> {
        return this.db.findOne<KeyStore>("keystores", { refresh_token_hash: hashRefreshToken }) as Promise<KeyStore | null>
    }
    async revokeFamily(family_id: string): Promise<void> {
        this.db.delete<KeyStore>("keystores", { family_id })
    }
    async markUsed(user_id: string): Promise<void> {
        this.db.update<KeyStore>("keystores", { user_id: user_id }, { is_used: true })
    }
    async markUsedByHash(hash: string): Promise<void> {
        this.db.update<KeyStore>("keystores", { refresh_token_hash: hash }, { is_used: true })
    }
    async revokeAllByUser(user_id: string): Promise<void> {
        this.db.update<KeyStore>("keystores", { is_used: false, user_id: user_id }, { is_used: true })
    }
}