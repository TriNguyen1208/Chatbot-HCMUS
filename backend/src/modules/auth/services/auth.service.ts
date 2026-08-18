import type { IAuthStrategy } from "#@/modules/auth/strategies/auth.strategy.js";
import { sha256 } from "#@/shared/utils/crypto.utils.js";
import type { KeystoreService } from "./keystore.service.js";

export class AuthService {
    constructor(
        private readonly keystoreService: KeystoreService
    ) {}

    async login(strategy: IAuthStrategy, credential: string) {
        return strategy.authenticate(credential)
    }
    async logout(rawRefreshToken: string) {
        const hashRefreshToken = sha256(rawRefreshToken)
        await this.keystoreService.markUsedByHash(hashRefreshToken)
    }
    async logoutAll(user_id: string) {
        await this.keystoreService.revokeAllByUser(user_id);
    }
}