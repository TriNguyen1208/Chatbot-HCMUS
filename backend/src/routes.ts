import { Router } from "express"
import authRoutes from "#@/modules/auth/auth.route.js"
import userRoutes from "#@/modules/user/user.route.js"
import messageRoutes from "#@/modules/message/message.route.js"
import conversationRoutes from "#@/modules/conversation/conversation.route.js"
import mediaRoutes from "#@/modules/media/media.route.js"
import searchRoutes from "#@/modules/search/search.route.js"

const router = Router()

router.use("/auth", authRoutes)
router.use("/user", userRoutes)
router.use("/message", messageRoutes)
router.use("/conversation", conversationRoutes)
router.use("/media", mediaRoutes)
router.use("/search", searchRoutes)

export default router