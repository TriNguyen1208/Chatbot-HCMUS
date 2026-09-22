import type { CronJobDefinition } from "../queue.types.js";
import { redisClient } from "#@/infrastructure/redis/redis.client.js";
import { socketManager } from "#@/infrastructure/websocket/socket.manager.js";
import { userFacade } from "#@/modules/user/user.facade.js";
import { SocketEvents } from "#@/infrastructure/websocket/socket.events.js";

export const syncPresenceCron: CronJobDefinition = {
    name: "sync_presence",
    cronExpression: "* * * * *", // Chạy định kỳ mỗi 1 phút một lần
    handler: async () => {
        try {
            // 1. Quét tất cả key user_sockets:* trong Redis để lấy danh sách users có active sockets
            const redis = redisClient.getClient();
            const socketStream = redis.scanStream({
                match: "user_sockets:*",
                count: 100
            });

            const userSocketKeys: string[] = [];
            for await (const keys of socketStream) {
                if (keys && keys.length > 0) {
                    userSocketKeys.push(...keys);
                }
            }

            // 2. Quét tất cả key presence:* trong Redis
            const presenceStream = redis.scanStream({
                match: "presence:*",
                count: 100
            });

            const presenceKeys: string[] = [];
            for await (const keys of presenceStream) {
                if (keys && keys.length > 0) {
                    presenceKeys.push(...keys);
                }
            }

            const candidateUserIds = new Set<string>([
                ...userSocketKeys.map(k => k.replace("user_sockets:", "")),
                ...presenceKeys.map(k => k.replace("presence:", ""))
            ]);

            if (candidateUserIds.size === 0) {
                return;
            }

            let fixedToOfflineCount = 0;
            let fixedToOnlineCount = 0;

            for (const userId of candidateUserIds) {
                const socketCount = await userFacade.isUserOnline(userId);
                const rawPresence = await redisClient.get(`presence:${userId}`);
                const isRedisOnline = rawPresence === "online";

                // Trường hợp 1: Không còn socket nào nhưng presence vẫn là online (Zombie Presence)
                if (!socketCount && isRedisOnline) {
                    const lastActive = new Date();

                    await userFacade.setPresenceOffline(userId, lastActive);

                    // Bắn socket thông báo qua Redis Emitter tới tất cả user
                    socketManager.emitToAll(SocketEvents.USER_OFFLINE, {
                        userId,
                        last_active: lastActive
                    });

                    fixedToOfflineCount++;
                }
                // Trường hợp 2: Có socket kết nối nhưng presence lại là offline hoặc thiếu key
                else if (socketCount && !isRedisOnline) {
                    await userFacade.setPresenceOnline(userId);

                    socketManager.emitToAll(SocketEvents.USER_ONLINE, { userId });

                    fixedToOnlineCount++;
                }
            }

            if (fixedToOfflineCount > 0 || fixedToOnlineCount > 0) {
                console.log(
                    `🔄 [Cron:sync_presence] Hoàn tất đối soát: Chuyển ${fixedToOfflineCount} zombie về offline, khôi phục ${fixedToOnlineCount} về online.`
                );
            }
        } catch (error) {
            console.error("❌ [Cron:sync_presence] Lỗi khi thực hiện đồng bộ presence:", error);
            throw error;
        }
    },
    options: {
        attempts: 2,
        removeOnComplete: 10,
        removeOnFail: 50
    }
};
