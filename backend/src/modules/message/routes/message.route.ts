import { Router } from "express";
import asyncHandler from "#@/shared/middlewares/asyncHandler.js";
import { AuthMiddleware } from "#@/shared/middlewares/auth.middleware.js";
import { validate } from "#@/shared/middlewares/validate.middleware.js";
import { SendMessageSchema, EditMessageSchema, MessageIdParamSchema, GetMessageListQuerySchema, ToggleReactionSchema, GetAllMessagesQuerySchema } from "../dto/message.dto.js";
import { messageContainer } from "../message.container.js";

const router = Router();

// API get messages globally across all conversations (for search)
router.get(
    "/",
    AuthMiddleware.verifyAccessToken,
    validate(GetAllMessagesQuerySchema),
    asyncHandler(messageContainer.messageController.getAllMessages)
);

// API get messages for a conversation
router.get(
    "/:conversation_id",
    AuthMiddleware.verifyAccessToken,
    validate(GetMessageListQuerySchema),
    asyncHandler(messageContainer.messageController.getMessages)
);

// API get context messages for a specific message
router.get(
    "/:conversation_id/context/:message_id",
    AuthMiddleware.verifyAccessToken,
    asyncHandler(messageContainer.messageController.getContextMessages)
);

// API send a message
router.post(
    "/",
    AuthMiddleware.verifyAccessToken,    
    validate(SendMessageSchema),
    asyncHandler(messageContainer.messageController.sendMessage)
);

// API edit a message
router.put(
    "/:id",
    AuthMiddleware.verifyAccessToken,
    validate(EditMessageSchema),
    asyncHandler(messageContainer.messageController.editMessage)
);

// API recall a message
router.delete(
    "/:id/recall",
    AuthMiddleware.verifyAccessToken,
    validate(MessageIdParamSchema),
    asyncHandler(messageContainer.messageController.recallMessage)
);

// API toggle a reaction
router.post(
    "/:id/reactions",
    AuthMiddleware.verifyAccessToken,
    validate(ToggleReactionSchema),
    asyncHandler(messageContainer.messageController.toggleReaction)
);

export default router;
