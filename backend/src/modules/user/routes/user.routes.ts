import asyncHandler from "#@/shared/middlewares/asyncHandler.js";
import { AuthMiddleware } from "#@/shared/middlewares/auth.middleware.js";
import { validate } from "#@/shared/middlewares/validate.middleware.js";
import { Router } from "express";
import { GetByIDParams, UpdateProfileSchema } from "#@/modules/user/user.dto.js";
import { UserController } from "../controllers/user.controller.js";
import { UserService } from "../services/user.service.js";
import { UserRepository } from "../repositories/user.repository.js";
import { supabaseDB } from "#@/infrastructure/database/supabaseClient.js";

const router = Router()
const userRepository = new UserRepository(supabaseDB)
const userService = new UserService(userRepository)
const userController = new UserController(userService)

router.get("/me", AuthMiddleware.verifyAccessToken, asyncHandler(userController.getMe))
router.get("/:id", validate(GetByIDParams), AuthMiddleware.verifyAccessToken, asyncHandler(userController.getByID))
router.patch("/", validate(UpdateProfileSchema), AuthMiddleware.verifyAccessToken, asyncHandler(userController.updateMe))

export default router