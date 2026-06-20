import {Router} from "express"
import {UserRepository} from "#@/repositories/user.repository.js"
import { AuthController } from "#@/controllers/auth.controller.js"
import { AuthService } from "#@/services/auth.service.js"
import { GoogleAuthStrategy } from "#@/strategies/auth.strategies.js"
import asyncHandler from "#@/middleware/asyncHandler.js"
import { AuthMiddleware } from "#@/middleware/auth.middleware.js"
import { KeyStoreRepository } from "#@/repositories/keystore.repository.js"
import { StudentDirectoryRepository } from "#@/repositories/student_directory.repository.js"

const router = Router()

//TODO: 

//Tầng controller sẽ có constructor của tầng services và repository (interface thôi)
//Services sử dụng repository và strategy (strategy ở đây là login bằng google hoặc bằng email)
//Controller sử dụng services và repository luôn

const userRepository = new UserRepository()
const studentDirectoryRepository = new StudentDirectoryRepository()
const keyStoreRepository = new KeyStoreRepository()
const authService = new AuthService(userRepository, keyStoreRepository) 
const authStrategy = new GoogleAuthStrategy(userRepository, studentDirectoryRepository)

const authController = new AuthController(authService, authStrategy)

//Login lần đầu vào google (Có thể là lần đầu, có thể là do hết refreshToken)
router.post("/google", asyncHandler(authController.googleLogin))

//Gửi refreshToken lên để lấy accessToken và refreshToken mới. Thì ở đây ta phải check refreshToken có hợp lệ không
router.post("/refresh-token", AuthMiddleware.verifyRefreshToken, authController.refreshToken)
//Khi hết accessToken thì vào đây
//Complete profile (Khi lần đầu vào thì nó trả về needProfiles = true, bắt buộc phải hoàn thành, nếu không thì không cho vào)

//Lấy thông tin cá nhân của người dùng
//Tất cả những thằng khác, khi vào thì phải qua middleware kiểm tra access và refresh. Nếu không qua thì cook


export default router