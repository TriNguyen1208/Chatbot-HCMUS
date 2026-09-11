import { searchContainer } from "./search.container.js";
import type { SearchResult } from "./search.dto.js";

export class SearchFacade {
    private get searchService() {
        return searchContainer.searchService;
    }

    async globalSearch(keyword: string, userId: string): Promise<SearchResult[]> {
        return this.searchService.globalSearch(keyword, userId);
    }

    async searchUsers(keyword: string) {
        return this.searchService.searchUsers(keyword);
    }

    async searchConversations(keyword: string, userId: string) {
        return this.searchService.searchConversations(keyword, userId);
    }

    async searchMessages(keyword: string, userId: string, conversationId?: string) {
        return this.searchService.searchMessages(keyword, userId, conversationId);
    }
}

export const searchFacade = new SearchFacade();
