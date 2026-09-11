import { Router } from "express"
import asyncHandler from "#@/shared/middlewares/async-handler.js";
import { AuthMiddleware } from "#@/shared/middlewares/auth.middleware.js"

import { GoogleLoginSchema } from "./auth.dto.js";
import { validate } from "#@/shared/middlewares/validate.middleware.js";
import { authContainer } from "./auth.container.js";
const router = Router()

router.post("/google", validate(GoogleLoginSchema), asyncHandler(authContainer.googleAuthController.login))

router.post("/microsoft", validate(GoogleLoginSchema), asyncHandler(authContainer.microsoftAuthController.login))

router.post("/refresh-token", AuthMiddleware.verifyRefreshToken, asyncHandler(authContainer.microsoftAuthController.refreshToken))

router.post("/logout", AuthMiddleware.verifyAccessToken, asyncHandler(authContainer.microsoftAuthController.logout))

router.post("/logout-all", AuthMiddleware.verifyAccessToken, asyncHandler(authContainer.microsoftAuthController.logoutAll))

export default router