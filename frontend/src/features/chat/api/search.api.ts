import { api } from "@/lib/api";

export interface SearchResult {
    search_type: 'user' | 'conversation' | 'message';
    id: string;
    text?: string;
    name?: string;
    avatar_url?: string;
    type?: string;
    sender?: {
        id: string;
        name: string;
        avatar_url: string;
    };
    conversation?: {
        id: string;
        name: string;
        avatar_url: string;
        type?: string;
    };
}

export const searchApi = {
    globalSearch: async (keyword: string): Promise<SearchResult[]> => {
        const response = await api.get('/search/global', {
            params: { q: keyword }
        });
        return response.data.data;
    }
};
