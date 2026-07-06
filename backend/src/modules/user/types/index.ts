import type { TokenPair } from "#@/shared/types/index.js";

export interface AuthResult {
    tokens: TokenPair;
    user: {
        id: string;
        email: string;
        name: string;
        student_id?: string;
    };
}

export type GoogleTokenPayload = {
  email: string;
  name: string;
  picture?: string | undefined; 
  sub: string;
}

export type KeyStore = {
    user_id: string,
    refresh_token_hash: string,
    family_id: string,
    parent_id?: string | null | undefined,
    is_used: boolean,
    device_info?: {
        user_agent?: string | undefined,
        ip?: string | undefined
    } | undefined,
    expires_at: Date
}

