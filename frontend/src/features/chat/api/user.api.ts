import { api } from "@/lib/api";

export interface User {
    id: string; 
    email: string; 
    name: string; 
    student_id?: string; 
    phone?: string; 
    is_online?: boolean; 
    avatar_url?: string; 
    created_at?: string; 
}

export const userApi = {
    getUsers: async (limit: number = 20, cursorId?: string) => {
        const params: Record<string, any> = { limit };
        if (cursorId) params.cursor_id = cursorId;
        
        const response = await api.get('/user', { params });
        return response.data;
    },

    getUserById: async (id: string) => {
        const response = await api.get(`/user/${id}`);
        return response.data;
    }
};
