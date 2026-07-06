import { config } from "#@/shared/config/config.js";
import type { IKeyStoreRepository } from "#@/modules/user/repositories/keystore.repository.js";
import type { IUserRepository } from "#@/modules/user/repositories/user.repository.js";
import type { IAuthStrategy } from "#@/modules/user/strategies/auth.strategies.js";
import type { JWTPayload, TokenPair } from "#@/shared/types/index.js";
import type { KeyStore } from "#@/modules/user/types/index.js";
import { sha256 } from "#@/shared/utils/crypto.utils.js";
import { parseDurationMs } from "#@/shared/utils/time.utils.js";
import {v4 as uuidv4} from 'uuid'
import { jwtService } from "#@/shared/utils/jwt-services.js";
import createHttpError from "http-errors";

export class AuthService{
    constructor(
        private readonly userRepository: IUserRepository,
        private readonly keyStoreRepository: IKeyStoreRepository
    ){
        this.userRepository = userRepository
        this.keyStoreRepository = keyStoreRepository
    }

    async login(strategy: IAuthStrategy, credential: string){
        return strategy.authenticate(credential)
    }
    async saveRefreshToken({
        userID,
        rawRefreshToken,
        device_info,
        family_id,
        parent_id
    }: {
        userID: string,
        rawRefreshToken: string,
        device_info?: {
            user_agent?: string | undefined,
            ip ?: string | undefined
        } | undefined,
        family_id?: string | undefined,
        parent_id?: string,
        
    }): Promise<void>{
        //Luu vao mongoDB va luu vao redis
        //Hash refreshToken
        const hashRefreshToken = sha256(rawRefreshToken)
        const expiresTime = new Date(
            Date.now() + parseDurationMs(config.jwt.refreshExpires as string)
        )
        
        //Lưu vào database mongoDB
        const payloadDatabase: KeyStore = {
            user_id: userID,
            refresh_token_hash: hashRefreshToken,
            family_id: family_id ?? uuidv4(),
            parent_id: parent_id ?? null,
            is_used: false,
            device_info: device_info,
            expires_at: expiresTime
        } 
        await this.keyStoreRepository.create(payloadDatabase)
    }

    async refreshToken({
        rawRefreshToken,
        deviceInfo,
        user
    }: {
        rawRefreshToken: string,
        deviceInfo?: { user_agent?: string | undefined; ip?: string | undefined },
        user: JWTPayload
    }): Promise<TokenPair>{
        //Hash rawRefreshToken trước
        const hashRefreshToken = sha256(rawRefreshToken)
        //Tim keyStore trong database
        const keyStore = await this.keyStoreRepository.findByHash(hashRefreshToken);
        //Neu nhu khong co trong database
        if(!keyStore){
            throw createHttpError.Unauthorized("Token không hợp lệ");
        }
        //Nếu như hết hạn thì throw error
        if(keyStore.expires_at < new Date()){
            throw createHttpError.Unauthorized("Phiên đăng nhập đã hết hạn, vui lòng đăng nhập lại");
        }
        //Nếu như đã dùng rồi thì throw error đồng thời xoá hết family_id của nó
        if(keyStore.is_used){
            await this.keyStoreRepository.revokeFamily(keyStore.family_id)
            throw createHttpError.Unauthorized("Phát hiện bất thường, vui lòng đăng nhập lại");
        }
        //Đánh dầu refreshToken đó đã dùng rồi, lần sau mà dùng lại thì cook
        await this.keyStoreRepository.markUsed(keyStore._id as any);
        
        //Tạo tokens mới
        const newTokens = jwtService.createPairToken({ id: user.userID, email: user.email });
        this.saveRefreshToken({
            userID: user.userID,
            rawRefreshToken: newTokens.refreshToken,
            device_info: deviceInfo,
            family_id: keyStore.family_id,
            parent_id: keyStore._id.toString()
        })
        return newTokens;    
    }
    async logout(rawRefreshToken: string){
        const hashRefreshToken = sha256(rawRefreshToken)
        await this.keyStoreRepository.markUsedByHash(hashRefreshToken)
    }
    async logoutAll(user_id: string){
        await this.keyStoreRepository.revokeAllByUser(user_id);
    }
}