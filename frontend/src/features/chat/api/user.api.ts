import { api } from "@/lib/api";

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
    },

    getBulkUsers: async (user_ids: string[]) => {
        const response = await api.post('/user/bulk', { user_ids });
        return response.data;
    }
};
