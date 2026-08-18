import type { AuthResult } from "#@/modules/auth/types/index.js";


export interface IAuthStrategy {
    /**
     * Authenticates a user based on the provided credential.
     * @param credential The credential to authenticate (e.g., an ID token).
     * @returns A promise resolving to the authentication result (tokens and user info).
     */
    authenticate(credential: string): Promise<AuthResult>
}