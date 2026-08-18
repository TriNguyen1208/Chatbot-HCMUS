import type { KeyStoreRepository } from "#@/modules/auth/repositories/keystore.repository.js"
import { config } from "#@/config/config.js";
import type { JWTPayload } from "#@/shared/types/index.js";
import type { KeyStore } from "#@/modules/auth/entities/keystore.entity.js";
import { sha256 } from "#@/shared/utils/crypto.utils.js";
import { parseDurationMs } from "#@/shared/utils/time.utils.js";
import { v4 as uuidv4 } from 'uuid'
import { jwtService } from "#@/shared/utils/jwt-services.js";
import createHttpError from "http-errors";

export class KeystoreService {
    constructor(
        private readonly keystoreRepo: KeyStoreRepository,
    ) { }

    /**
     * Saves a refresh token into the keystore database.
     * @param params Object containing the refresh token payload.
     * @param params.userID The ID of the user.
     * @param params.rawRefreshToken The raw (unhashed) refresh token.
     * @param params.device_info Optional device info (user agent, IP).
     * @param params.family_id Optional family ID to group tokens in the same hierarchy.
     * @param params.parent_id Optional parent ID referencing the previous token in the chain.
     * @param params.expires_at Optional expiration date. If not provided, it will be calculated based on config.
     */
    async saveRefreshToken({
        userID,
        rawRefreshToken,
        device_info,
        family_id,
        parent_id,
        expires_at
    }: {
        userID: string,
        rawRefreshToken: string,
        device_info?: {
            user_agent?: string | undefined,
            ip?: string | undefined
        } | undefined,
        family_id?: string | undefined,
        parent_id?: string | null,
        expires_at?: Date

    }): Promise<void> {
        const hashRefreshToken = sha256(rawRefreshToken)
        if (!expires_at) {
            expires_at = new Date(
                Date.now() + parseDurationMs(config.jwt.refreshExpires as string)
            )
        }
        // Save to MongoDB database
        const payloadDatabase: KeyStore = {
            user_id: userID,
            refresh_token_hash: hashRefreshToken,
            family_id: family_id ?? uuidv4(),
            parent_id: parent_id ?? null,
            is_used: false,
            device_info: device_info,
            expires_at: expires_at
        }
        await this.keystoreRepo.create(payloadDatabase)
    }

    /**
     * Validates an existing refresh token, revokes it, and issues a new pair of tokens.
     * Implements token rotation and family revocation if reuse is detected.
     * @param params Object containing refresh parameters.
     * @param params.rawRefreshToken The raw refresh token from the client.
     * @param params.deviceInfo Optional device info from the incoming request.
     * @param params.user The decoded JWT payload of the user.
     * @returns An object containing the new tokens and their expiration time.
     * @throws Unauthorized if token is invalid, expired, or reused.
     */
    async refreshToken({
        rawRefreshToken,
        deviceInfo,
        user
    }: {
        rawRefreshToken: string,
        deviceInfo?: { user_agent?: string | undefined; ip?: string | undefined },
        user: JWTPayload
    }) {
        const hashRefreshToken = sha256(rawRefreshToken)
        const keyStore = await this.keystoreRepo.findByHash(hashRefreshToken);
        if (!keyStore) {
            throw createHttpError.Unauthorized("Invalid Token");
        }
        if (keyStore.expires_at < new Date()) {
            await this.keystoreRepo.markUsedByHash(hashRefreshToken);
            throw createHttpError.Unauthorized("Login session has expired, please log in again");
        }
        if (keyStore.is_used) {
            await this.keystoreRepo.revokeFamily(keyStore.family_id)
            throw createHttpError.Unauthorized("Anomaly detected, please log in again");
        }
        const expiresTime = keyStore.expires_at
        await this.keystoreRepo.markUsedByHash(hashRefreshToken);

        const newTokens = jwtService.createPairToken({ id: user.userID, email: user.email });
        await this.saveRefreshToken({
            userID: user.userID,
            rawRefreshToken: newTokens.refreshToken,
            device_info: deviceInfo,
            family_id: keyStore.family_id,
            parent_id: keyStore.parent_id,
            expires_at: expiresTime
        })
        return {
            tokens: newTokens,
            expires_refresh_token: expiresTime
        }
    }

    /**
     * Marks a refresh token as used by its hash.
     * @param hashRefreshToken The hashed refresh token.
     */
    async markUsedByHash(hashRefreshToken: string): Promise<void> {
        await this.keystoreRepo.markUsedByHash(hashRefreshToken)
    }

    /**
     * Revokes all refresh tokens associated with a specific user.
     * @param user_id The ID of the user.
     */
    async revokeAllByUser(user_id: string) {
        await this.keystoreRepo.revokeAllByUser(user_id)
    }
}