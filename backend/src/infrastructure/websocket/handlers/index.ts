import type { Server, Socket } from "socket.io";
import type { SocketManager } from "../socket.manager.js";
import { registerUserSocket } from "#@/modules/user/user.socket.js";
import { registerMessageSocket } from "#@/modules/message/message.socket.js";
import { registerConversationSocket } from "#@/modules/conversation/conversation.socket.js";

/**
 * Đăng ký tất cả các Socket Handlers theo từng Feature Module:
 * - User: Presence, Online/Offline
 * - Message: Chat, Typing, Watermark (Delivered/Read)
 * - Conversation: Group management, Members, Status
 */
export const registerSocketHandlers = (
    io: Server,
    socket: Socket,
    socketManager: SocketManager,
    allUsersRoom: string
): void => {
    registerUserSocket(io, socket, socketManager, allUsersRoom);
    registerMessageSocket(socket, socketManager);
    registerConversationSocket(socket, socketManager);
};
