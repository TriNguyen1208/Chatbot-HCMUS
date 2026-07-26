export interface KeyStore {
    user_id: string,
    refresh_token_hash: string,
    family_id: string,
    parent_id?: string | null,
    is_used: boolean | false,
    device_info?: {
        user_agent?: string,
        ip?: string
    } | null,
    expires_at: Date
}