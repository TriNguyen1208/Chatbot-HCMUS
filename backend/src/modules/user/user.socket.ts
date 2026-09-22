import type { Server, Socket } from "socket.io";
import type { ISocketManager } from "#@/infrastructure/websocket/socket.manager.js";
import { SocketEvents } from "#@/infrastructure/websocket/socket.events.js";
import { userFacade } from "#@/modules/user/user.facade.js";

/**
 * Quản lý các sự kiện Socket liên quan đến User (Presence, Online/Offline)
 */
export const registerUserSocket = (
    io: Server,
    socket: Socket,
    socketManager: ISocketManager,
    allUsersRoom: string
): void => {
    const userId = socket.data.userId as string;
    if (!userId) return;

    // Track socket ID in Redis Set & update presence online
    userFacade.onUserSocketConnected(userId, socket.id).catch(err => {
        console.error("[Socket.IO] Failed to track connected socket", err);
    });

    // Broadcast to other users
    socket.broadcast.to(allUsersRoom).emit(SocketEvents.USER_ONLINE, { userId });

    // Handle disconnect
    socket.on("disconnect", (reason) => {
        console.log(`[Socket.IO] User '${userId}' disconnected. Socket: '${socket.id}', reason: '${reason}'`);

        // Debounce for 3 seconds to handle page refresh / multiple tabs
        setTimeout(async () => {
            try {
                const remainingSockets = await userFacade.onUserSocketDisconnected(userId, socket.id);
                if (remainingSockets === 0) {
                    const lastActive = new Date();

                    // Update presence in Redis to offline & update DB via Facade
                    await userFacade.setPresenceOffline(userId, lastActive);

                    // Broadcast offline event to all other users
                    socketManager.emitToAll(SocketEvents.USER_OFFLINE, {
                        userId,
                        last_active: lastActive
                    });
                }
            } catch (error) {
                console.error("[Socket.IO] Error handling disconnect presence:", error);
            }
        }, 3000);
    });
};
