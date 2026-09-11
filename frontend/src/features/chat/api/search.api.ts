import { http } from "@/lib/api";

export interface SearchResult {
    search_type: 'user' | 'conversation' | 'message';
    id: string;
    content?: string;
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
        return http.get<SearchResult[]>('/search/global', {
            params: { q: keyword }
        });
    }
};
