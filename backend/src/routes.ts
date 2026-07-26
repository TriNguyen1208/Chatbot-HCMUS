import {Router} from "express"
import authRoutes from "#@/modules/user/routes/auth.routes.js"
import userRoutes from "#@/modules/user/routes/user.routes.js"

const router = Router()

router.use("/auth", authRoutes)
router.use("/user", userRoutes)

export default router