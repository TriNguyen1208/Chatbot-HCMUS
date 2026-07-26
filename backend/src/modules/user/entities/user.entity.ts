export interface User {
    id: string;
    email: string;
    name: string;
    student_id?: string;
    phone?: string;
    avatar_url?: string;
    is_online?: boolean | false,
    created_at?: Date;
    updated_at?: Date;
}