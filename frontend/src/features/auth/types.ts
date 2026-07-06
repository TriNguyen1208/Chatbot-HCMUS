export type TokenPair = {
    accessToken: string,
    refreshToken: string
}

export type UserProfile = {
    id: string;
    name: string;
    email: string;
    studentID?: string;
    phone?: string;
    avatarUrl?: string;
}
export type AuthResult = {
    tokens: TokenPair,
    user: Pick<UserProfile, "id" | "name" | "email">
}
