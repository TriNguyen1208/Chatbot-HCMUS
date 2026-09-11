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
    NEW_MESSAGE: "new_message",
} as const;

export type SocketEvent = typeof SocketEvents[keyof typeof SocketEvents];
