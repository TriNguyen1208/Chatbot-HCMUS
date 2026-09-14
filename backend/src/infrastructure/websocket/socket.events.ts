export const SocketEvents = {
    // Client to Server
    TYPING: "typing",
    STOP_TYPING: "stop_typing",
    MARK_DELIVERED: "mark_delivered",
    MARK_READ: "mark_read",

    // Server to Client
    USER_ONLINE: "user_online",
    USER_OFFLINE: "user_offline",
    WATERMARK_UPDATED: "watermark_updated",
    MESSAGE_SAVE_FAILED: "message_save_failed",

    // Full-Duplex (Client to Server & Server to Client)
    NEW_MESSAGE: "new_message",
    EDIT_MESSAGE: "message_edited",
    RECALL_MESSAGE: "message_recalled",
    REACTION_MESSAGE: "message_reaction_updated",
    NEW_CONVERSATION: "new_conversation",
    UPDATE_CONVERSATION: "conversation_updated",
    CREATE_CONVERSATION: "conversation_updated", // alias for compatibility
    ADD_MEMBER_CONVERSATION: "members_added",
    KICK_MEMBER_CONVERSATION: "members_kicked",
    UPDATE_ADMIN: "admins_updated",
    MEMBER_LEFT: "member_left",
    BLOCK_CONVERSATION: "conversation_blocked",
    UNBLOCK_CONVERSATION: "conversation_unblocked",
    DISBAND_GROUP: "group_disbanded"
} as const;

export type SocketEvent = typeof SocketEvents[keyof typeof SocketEvents];
