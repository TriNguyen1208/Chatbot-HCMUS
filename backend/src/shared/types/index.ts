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
