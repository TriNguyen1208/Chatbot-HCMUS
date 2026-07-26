import { Router } from "express"
import { UserRepository } from "#@/modules/user/repositories/user.repository.js"
import { AuthController } from "#@/modules/user/controllers/auth.controller.js"
import { AuthService } from "#@/modules/user/services/auth.service.js"
import { GoogleAuthStrategy } from "#@/modules/user/strategies/auth.strategy.js"
import asyncHandler from "#@/shared/middlewares/asyncHandler.js"
import { AuthMiddleware } from "#@/shared/middlewares/auth.middleware.js"
import { KeyStoreRepository } from "#@/modules/user/repositories/keystore.repository.js"
import { StudentDirectoryRepository } from "#@/modules/user/repositories/student-directory.repository.js"
import { GoogleLoginSchema } from "#@/modules/user/user.dto.js";
import { validate } from "#@/shared/middlewares/validate.middleware.js";
import { supabaseDB } from "#@/infrastructure/database/supabaseClient.js"
const router = Router()

//TODO: 

//Tầng controller sẽ có constructor của tầng services và repository (interface thôi)
//Services sử dụng repository và strategy (strategy ở đây là login bằng google hoặc bằng email)
//Controller sử dụng services và repository luôn

const userRepository = new UserRepository(supabaseDB)
const studentDirectoryRepository = new StudentDirectoryRepository(supabaseDB)
const keyStoreRepository = new KeyStoreRepository(supabaseDB)

const authService = new AuthService(userRepository, keyStoreRepository)
const authStrategy = new GoogleAuthStrategy(userRepository, studentDirectoryRepository)

const authController = new AuthController(authService, authStrategy)

//Login lần đầu vào google (Có thể là lần đầu, có thể là do hết refreshToken)
router.post("/google", validate(GoogleLoginSchema), asyncHandler(authController.googleLogin))

router.post("/refresh-token", AuthMiddleware.verifyRefreshToken, authController.refreshToken)

router.post("/logout", AuthMiddleware.verifyAccessToken, asyncHandler(authController.logout))

router.post("/logout-all", AuthMiddleware.verifyAccessToken, asyncHandler(authController.logoutAll))

router.get("/me", AuthMiddleware.verifyAccessToken, asyncHandler(authController.getMe))

export default router