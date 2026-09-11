import { http } from "@/lib/api";
import type { User } from "@/types";

export const userApi = {
    getUsers: async (limit: number = 20, cursorId?: string): Promise<User[]> => {
        const params: Record<string, any> = { limit };
        if (cursorId) params.cursor_id = cursorId;
        return http.get<User[]>('/user', { params });
    },

    getUserById: async (id: string): Promise<User> => {
        return http.get<User>(`/user/${id}`);
    },

    getBulkUsers: async (user_ids: string[]): Promise<User[]> => {
        return http.post<User[]>('/user/bulk', { user_ids });
    }
};
