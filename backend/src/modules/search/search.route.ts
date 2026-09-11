import { Router } from 'express';
import { searchContainer } from './search.container.js';
import { AuthMiddleware } from '#@/shared/middlewares/auth.middleware.js';
import asyncHandler from '#@/shared/middlewares/async-handler.js';

const router = Router();

// Áp dụng middleware kiểm tra đăng nhập cho toàn bộ các route liên quan đến tính năng tìm kiếm
router.use(AuthMiddleware.verifyAccessToken);

// Endpoint tìm kiếm toàn cục
router.get('/global', asyncHandler(searchContainer.searchController.globalSearch));

export default router;
