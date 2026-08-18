import { Router } from "express"
import authRoutes from "#@/modules/auth/routes/auth.route.js"
import userRoutes from "#@/modules/user/routes/user.route.js"
import messageRoutes from "#@/modules/message/routes/message.route.js"
import conversationRoutes from "#@/modules/conversation/routes/conversation.route.js"
import mediaRoutes from "#@/modules/media/routes/media.route.js"

const router = Router()

router.use("/auth", authRoutes)
router.use("/user", userRoutes)
router.use("/message", messageRoutes)
router.use("/conversation", conversationRoutes)
router.use("/media", mediaRoutes)

export default router