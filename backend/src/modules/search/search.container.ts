import { esClient } from "#@/infrastructure/elasticsearch/es.client.js";
import { SearchService } from "./search.service.js";
import { SearchController } from "./search.controller.js";

class SearchContainer {
    private _searchService?: SearchService;
    public get searchService(): SearchService {
        if (!this._searchService) {
            this._searchService = new SearchService(esClient);
        }
        return this._searchService;
    }

    private _searchController?: SearchController;
    public get searchController(): SearchController {
        if (!this._searchController) {
            this._searchController = new SearchController(this.searchService);
        }
        return this._searchController;
    }
}

export const searchContainer = new SearchContainer();
