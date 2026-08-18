import { Router } from "express"
import asyncHandler from "#@/shared/middlewares/asyncHandler.js"
import { AuthMiddleware } from "#@/shared/middlewares/auth.middleware.js"

import { GoogleLoginSchema } from "#@/modules/auth/dto/auth.dto.js";
import { validate } from "#@/shared/middlewares/validate.middleware.js";
import { authContainer } from "#@/modules/auth/auth.container.js";
const router = Router()

router.post("/google", validate(GoogleLoginSchema), asyncHandler(authContainer.authGoogleControllerTest.googleLogin))

router.post("/microsoft", validate(GoogleLoginSchema), asyncHandler(authContainer.authController.googleLogin))

router.post("/refresh-token", AuthMiddleware.verifyRefreshToken, asyncHandler(authContainer.authController.refreshToken))

router.post("/logout", AuthMiddleware.verifyAccessToken, asyncHandler(authContainer.authController.logout))

router.post("/logout-all", AuthMiddleware.verifyAccessToken, asyncHandler(authContainer.authController.logoutAll))

export default router