import { z } from "zod";

export interface SearchResult {
    search_type: 'user' | 'conversation' | 'message';
    id: string;
    name?: string;
    avatar_url?: string;
    text?: string;
    sender?: {
        id?: string;
        name?: string;
        avatar_url?: string;
    };
    conversation?: {
        id?: string;
        name?: string;
        avatar_url?: string;
        type?: string;
    };
}

export const GlobalSearchQuerySchema = z.object({
    query: z.object({
        q: z.string().optional(),
        search: z.string().optional(),
    }).refine(data => data.q || data.search, {
        message: 'Query parameter "search" or "q" is required'
    })
});
