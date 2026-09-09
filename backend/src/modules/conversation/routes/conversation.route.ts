import { Router } from "express";
import asyncHandler from "#@/shared/middlewares/asyncHandler.js";
import { AuthMiddleware } from "#@/shared/middlewares/auth.middleware.js";
import { validate } from "#@/shared/middlewares/validate.middleware.js";
import { 
    CreateConversationSchema, 
    GetConversationParamSchema, 
    AddMembersSchema,
    RemoveMemberSchema,
    AssignAdminSchema, 
    GetListQuerySchema,
    UpdateConversationSchema
} from "#@/modules/conversation/dto/conversation.dto.js"
import { conversationContainer } from "#@/modules/conversation/conversation.container.js";
const router = Router();

// API Update conversation
router.put(
    "/:id",
    AuthMiddleware.verifyAccessToken,
    validate(UpdateConversationSchema),
    asyncHandler(conversationContainer.conversationController.updateConversation)
);

// API Get conversation list
router.get(
    "/",
    AuthMiddleware.verifyAccessToken,
    validate(GetListQuerySchema),
    asyncHandler(conversationContainer.conversationController.getList)
);

// API Create a new conversation
router.post(
    "/",
    AuthMiddleware.verifyAccessToken,
    validate(CreateConversationSchema),
    asyncHandler(conversationContainer.conversationController.createConversation)
);

// API Get conversation information by ID
router.get(
    "/:id",
    AuthMiddleware.verifyAccessToken,
    validate(GetConversationParamSchema),
    asyncHandler(conversationContainer.conversationController.getConversation)
);

// API Add member to group
router.post(
    "/:id/members",
    AuthMiddleware.verifyAccessToken,
    validate(AddMembersSchema),
    asyncHandler(conversationContainer.conversationController.addMember)
);

// API Assign admins in group
router.post(
    "/:id/admins",
    AuthMiddleware.verifyAccessToken,
    validate(AssignAdminSchema),
    asyncHandler(conversationContainer.conversationController.assignAdmins)
);

// API Remove member from group
router.delete(
    "/:id/members",
    AuthMiddleware.verifyAccessToken,
    validate(RemoveMemberSchema),
    asyncHandler(conversationContainer.conversationController.removeMember)
);

// API Leave group
router.post(
    "/:id/leave",
    AuthMiddleware.verifyAccessToken,
    asyncHandler(conversationContainer.conversationController.leaveGroup)
);

// API Block 1-1 conversation
router.post(
    "/:id/block",
    AuthMiddleware.verifyAccessToken,
    validate(GetConversationParamSchema),
    asyncHandler(conversationContainer.conversationController.blockConversation)
);

// API Unblock 1-1 conversation
router.post(
    "/:id/unblock",
    AuthMiddleware.verifyAccessToken,
    validate(GetConversationParamSchema),
    asyncHandler(conversationContainer.conversationController.unblockConversation)
);

// API Disband group conversation
router.delete(
    "/:id",
    AuthMiddleware.verifyAccessToken,
    validate(GetConversationParamSchema),
    asyncHandler(conversationContainer.conversationController.disbandGroup)
);

export default router;
