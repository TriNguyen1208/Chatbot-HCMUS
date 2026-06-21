// export type AuthResult = {
//     needsProfile: true;
//     userID: string;
// } | {
//     needsProfile: false;
//     tokens: TokenPair;
//     user: Pick<UserProfile, "id" | "email" | "name">;
// }

export type AuthResult = {
    tokens: TokenPair;
    user: Pick<UserProfile, "id" | "name" | "email"> & {
        student_id?: string | undefined; 
    };
};

export type GoogleTokenPayload = {
  email: string;
  name: string;
  picture?: string | undefined; 
  sub: string;
}

export type TokenPair = {
    accessToken: string, 
    refreshToken: string
}
export type UserProfile = {
    id: string,
    name: string,
    email: string,
    student_id?: string | undefined,
    phone?: string | undefined,
    avatarUrl?: string | undefined,
    createdAt: Date,
    updatedAt: Date
}

export interface JWTPayload{
    userID: string,
    email: string,
    iat?: number, //Issues At
    exp?: number //Expired Time
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
export interface RedisCachePayload {
  userID: string;
  email: string;
  family_id: string;
}