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
