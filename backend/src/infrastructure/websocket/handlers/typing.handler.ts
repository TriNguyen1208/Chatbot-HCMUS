import type { Socket } from "socket.io";
import type { SocketManager } from "../socket.manager.js";
import { SocketEvents } from "../socket.events.js";
import type { TypingPayload, StopTypingPayload } from "../socket.types.js";

export const registerTypingHandler = (socket: Socket, socketManager: SocketManager): void => {
    const userId = socket.data.userId as string;

    socket.on(SocketEvents.TYPING, (data: TypingPayload) => {
        if (!data?.conversationId || !data?.receiverIds || !Array.isArray(data.receiverIds)) return;

        socketManager.emitToUsers(data.receiverIds, SocketEvents.TYPING, {
            conversationId: data.conversationId,
            userId,
            name: data.name || "Ai đó"
        });
    });

    socket.on(SocketEvents.STOP_TYPING, (data: StopTypingPayload) => {
        if (!data?.conversationId || !data?.receiverIds || !Array.isArray(data.receiverIds)) return;

        socketManager.emitToUsers(data.receiverIds, SocketEvents.STOP_TYPING, {
            conversationId: data.conversationId,
            userId
        });
    });
};
