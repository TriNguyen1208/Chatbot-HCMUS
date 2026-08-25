import { type IDatabase } from "#@/infrastructure/database/database.interface.js";
import { type KeyStore, type KeyStoreDB } from "#@/modules/auth/entities/keystore.entity.js";

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

    private mapToDomain(doc: KeyStoreDB | null): KeyStore | null {
        if (!doc) return null;
        const { _id, __v, ...rest } = doc;
        return {
            id: _id?.toString(),
            ...rest
        } as KeyStore;
    }

    async create(data: KeyStore): Promise<KeyStore> {
        const doc = await this.db.insert<KeyStoreDB>("keystores", data as KeyStoreDB);
        return this.mapToDomain(doc as KeyStoreDB | null) as KeyStore;
    }
    async findByHash(hashRefreshToken: string): Promise<KeyStore | null> {
        const doc = await this.db.findOne<KeyStoreDB>("keystores", { refresh_token_hash: hashRefreshToken });
        return this.mapToDomain(doc);
    }
    async revokeFamily(family_id: string): Promise<void> {
        await this.db.delete<KeyStoreDB>("keystores", { family_id })
    }
    async markUsed(user_id: string): Promise<void> {
        await this.db.update<KeyStoreDB>("keystores", { user_id: user_id }, { is_used: true })
    }
    async markUsedByHash(hash: string): Promise<void> {
        await this.db.update<KeyStoreDB>("keystores", { refresh_token_hash: hash }, { is_used: true })
    }
    async revokeAllByUser(user_id: string): Promise<void> {
        await this.db.update<KeyStoreDB>("keystores", { is_used: false, user_id: user_id }, { is_used: true })
    }
}