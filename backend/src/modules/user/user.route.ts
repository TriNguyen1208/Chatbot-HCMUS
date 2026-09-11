import asyncHandler from "#@/shared/middlewares/async-handler.js";
import { AuthMiddleware } from "#@/shared/middlewares/auth.middleware.js";
import { validate } from "#@/shared/middlewares/validate.middleware.js";
import { Router } from "express";
import { GetByIDParams, UpdateProfileSchema, GetListQuery, GetBulkBodySchema } from "./user.dto.js";
import { userContainer } from "./user.container.js";

const router = Router()

router.get("/me", AuthMiddleware.verifyAccessToken, asyncHandler(userContainer.userController.getMe))
router.get("/", validate(GetListQuery), AuthMiddleware.verifyAccessToken, asyncHandler(userContainer.userController.getList))
router.post("/bulk", validate(GetBulkBodySchema), AuthMiddleware.verifyAccessToken, asyncHandler(userContainer.userController.getBulk))
router.get("/:id", validate(GetByIDParams), AuthMiddleware.verifyAccessToken, asyncHandler(userContainer.userController.getByID))
router.patch("/", validate(UpdateProfileSchema), AuthMiddleware.verifyAccessToken, asyncHandler(userContainer.userController.updateMe))

export default router