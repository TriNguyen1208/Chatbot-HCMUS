import { Router } from 'express';
import { SearchController } from './search.controller.js';
import { AuthMiddleware } from '#@/shared/middlewares/auth.middleware.js';
import asyncHandler from '#@/shared/middlewares/asyncHandler.js';

const router = Router();

// Áp dụng middleware kiểm tra đăng nhập cho toàn bộ các route liên quan đến tính năng tìm kiếm (Bảo mật)
router.use(AuthMiddleware.verifyAccessToken);

// Định nghĩa endpoint để Frontend có thể gọi lên
router.get('/global', asyncHandler(SearchController.globalSearch));

export default router;
