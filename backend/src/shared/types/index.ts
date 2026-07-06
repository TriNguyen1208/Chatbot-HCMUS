export interface JWTPayload{
    userID: string,
    email: string,
    iat?: number, //Issues At
    exp?: number //Expired Time
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