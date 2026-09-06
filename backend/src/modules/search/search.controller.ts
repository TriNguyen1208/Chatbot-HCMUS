import type { Request, Response } from 'express';
import { SearchService } from './search.service.js';
import { ZodError } from 'zod';
import { apiResponse } from '#@/shared/utils/api-response.js';
import createHttpError from 'http-errors';

export class SearchController {
    // API dành cho thanh Search toàn cục (Global Search)
    static async globalSearch(req: Request, res: Response) {
        // Lấy từ khoá tìm kiếm từ query params (hỗ trợ cả ?search=abc và ?q=abc)
        const { q, search } = req.query;
        const keyword = (search || q) as string;
        if (!keyword) {
            throw createHttpError.BadRequest('Query parameter "search" or "q" is required');
        }

        const userId = req.user!.userID;

        // Gọi logic tìm kiếm đa index từ Service
        const results = await SearchService.globalSearch(keyword, userId);
        
        // Trả về JSON thành công
        return apiResponse.success(res, results, { statusCode: 200, message: 'Search successful' });
    }
}
