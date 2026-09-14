import type { Server, Socket } from "socket.io";
import type { SocketManager } from "#@/infrastructure/websocket/socket.manager.js";
import { SocketEvents } from "#@/infrastructure/websocket/socket.events.js";
import { userFacade } from "#@/modules/user/user.facade.js";

/**
 * Quản lý các sự kiện Socket liên quan đến User (Presence, Online/Offline)
 */
export const registerUserSocket = (
    io: Server,
    socket: Socket,
    socketManager: SocketManager,
    allUsersRoom: string
): void => {
    const userId = socket.data.userId as string;
    if (!userId) return;

    // Update presence (24h TTL) via Facade
    userFacade.setPresenceOnline(userId).catch(err => {
        console.error("[Socket.IO] Failed to set redis presence", err);
    });

    // Broadcast to other users
    socket.broadcast.to(allUsersRoom).emit(SocketEvents.USER_ONLINE, { userId });

    // Handle disconnect
    socket.on("disconnect", (reason) => {
        console.log(`[Socket.IO] User '${userId}' disconnected. Socket: '${socket.id}', reason: '${reason}'`);

        // Debounce for 3 seconds to handle page refresh / multiple tabs
        setTimeout(async () => {
            try {
                const isOnline = await socketManager.isUserOnline(userId);
                if (!isOnline) {
                    const lastActive = new Date();

                    // Update presence in Redis to offline & update DB via Facade
                    await userFacade.setPresenceOffline(userId, lastActive);

                    // Broadcast offline event to all other users
                    io.to(allUsersRoom).emit(SocketEvents.USER_OFFLINE, {
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
