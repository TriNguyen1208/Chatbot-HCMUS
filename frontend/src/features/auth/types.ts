export type TokenPair = {
    accessToken: string,
    refreshToken: string
}

export type UserProfile = {
    id: string;
    name: string;
    email: string;
    student_id?: string;
    phone?: string;
    avatar_url?: string;
}
export type AuthResult = {
    tokens: TokenPair,
    user: Pick<UserProfile, "id" | "name" | "email" | "avatar_url">
}
