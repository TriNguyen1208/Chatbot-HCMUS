import type { Request, Response } from 'express';
import type { SearchService } from './search.service.js';
import { apiResponse } from '#@/shared/utils/api-response.util.js';
import createHttpError from 'http-errors';

export class SearchController {
    constructor(private readonly searchService: SearchService) {}

    // API dành cho thanh Search toàn cục (Global Search)
    globalSearch = async (req: Request, res: Response) => {
        // Lấy từ khoá tìm kiếm từ query params (hỗ trợ cả ?search=abc và ?q=abc)
        const { q, search } = req.query;
        const keyword = (search || q) as string;
        if (!keyword) {
            throw createHttpError.BadRequest('Query parameter "search" or "q" is required');
        }

        const userId = req.user!.userID;

        // Gọi logic tìm kiếm đa index từ Service
        const results = await this.searchService.globalSearch(keyword, userId);

        // Trả về JSON thành công
        return apiResponse.success(res, results, { statusCode: 200, message: 'Search successful' });
    };
}
