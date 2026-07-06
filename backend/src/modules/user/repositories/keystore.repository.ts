import { keyStoreModel, type IKeyStore } from "#@/modules/user/models/keystore.model.js"
import { type KeyStore } from "#@/modules/user/types/index.js";
import {string2ObjectID} from '#@/shared/utils/mongo.utils.js'
import { Types } from "mongoose";

export interface IKeyStoreRepository{
    create(data: KeyStore): Promise<IKeyStore>
    findByHash(hashRefreshToken: string): Promise<IKeyStore | null>
    revokeFamily(family_id: string): Promise<void>
    markUsed(id: Types.ObjectId): Promise<void>
    markUsedByHash(hash: string): Promise<void>
    revokeAllByUser(user_id: string): Promise<void>
}


export class KeyStoreRepository implements IKeyStoreRepository{
    async create(data: KeyStore): Promise<IKeyStore> {
        return await keyStoreModel.create({
            user_id: string2ObjectID(data.user_id)!,
            refresh_token_hash: data.refresh_token_hash,
            family_id: data.family_id,
            parent_id: string2ObjectID(data.parent_id!),
            is_used: false,
            device_info: data.device_info,
            expires_at: data.expires_at
        })
    }
    async findByHash(hashRefreshToken: string): Promise<IKeyStore | null>{
        return await keyStoreModel.findOne({
            refresh_token_hash: hashRefreshToken
        })
    }
    async revokeFamily(family_id: string): Promise<void> {
        await keyStoreModel.deleteMany({family_id});
    }
    async markUsed(id: Types.ObjectId): Promise<void> {
        await keyStoreModel.findByIdAndUpdate(id, {
            is_used: true
        }).lean()
    }
    async markUsedByHash(hash: string): Promise<void> {
        await keyStoreModel.updateOne({refresh_token_hash: hash}, {is_used: true});
    }
    async revokeAllByUser(user_id: string): Promise<void>{
        await keyStoreModel.updateMany(
            { user_id: new Types.ObjectId(user_id), is_used: false },
            { is_used: true }
        );
    }
}