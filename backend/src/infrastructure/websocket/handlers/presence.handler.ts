import type { Server, Socket } from "socket.io";
import type { SocketManager } from "../socket.manager.js";
import { SocketEvents } from "../socket.events.js";
import { redisClient } from "#@/infrastructure/redis/redis.client.js";
import { userFacade } from "#@/modules/user/user.facade.js";

export const registerPresenceHandler = (
    io: Server,
    socket: Socket,
    socketManager: SocketManager,
    allUsersRoom: string
): void => {
    const userId = socket.data.userId as string;

    // Update presence in Redis (24h TTL)
    redisClient.set(`presence:${userId}`, "online", 24 * 3600).catch(err => {
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

                    // Update presence in Redis to offline
                    await redisClient.setJSON(`presence:${userId}`, {
                        status: 'offline',
                        last_active: lastActive
                    }, 24 * 3600);

                    // Update database via Facade
                    await userFacade.updatePresence(userId, lastActive);

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
