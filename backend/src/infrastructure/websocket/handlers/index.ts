import type { Server, Socket } from "socket.io";
import type { SocketManager } from "../socket.manager.js";
import { registerPresenceHandler } from "./presence.handler.js";
import { registerTypingHandler } from "./typing.handler.js";
import { registerWatermarkHandler } from "./watermark.handler.js";

export const registerSocketHandlers = (
    io: Server,
    socket: Socket,
    socketManager: SocketManager,
    allUsersRoom: string
): void => {
    registerPresenceHandler(io, socket, socketManager, allUsersRoom);
    registerTypingHandler(socket, socketManager);
    registerWatermarkHandler(socket, socketManager);
};
