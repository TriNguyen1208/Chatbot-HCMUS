import type { CronJobDefinition } from "../queue.types.js";
import { redisClient } from "#@/infrastructure/redis/redis.client.js";
import { socketManager } from "#@/infrastructure/websocket/socket.manager.js";
import { userFacade } from "#@/modules/user/user.facade.js";

const ALL_USERS_ROOM = "global:all_users";

export const syncPresenceCron: CronJobDefinition = {
    name: "sync_presence",
    cronExpression: "* * * * *", // Chạy định kỳ mỗi 1 phút một lần
    handler: async () => {
        try {
            const io = socketManager?.getIO();
            if (!io) {
                console.warn("⚠️ [Cron:sync_presence] Socket.IO chưa sẵn sàng, bỏ qua chu kỳ này.");
                return;
            }

            // 1. Quét tất cả socket đang kết nối để lấy danh sách User IDs thực sự đang online
            const activeSockets = await io.fetchSockets();
            const activeSocketUserIds = new Set<string>();

            for (const socket of activeSockets) {
                const uid = socket.data?.userId as string | undefined;
                if (uid) {
                    activeSocketUserIds.add(uid);
                }
            }

            // 2. Quét tất cả key presence:* hiện có trong Redis bằng SCAN (non-blocking)
            const redis = redisClient.getClient();
            const stream = redis.scanStream({
                match: "presence:*",
                count: 100
            });

            const presenceKeys: string[] = [];
            for await (const keys of stream) {
                if (keys && keys.length > 0) {
                    presenceKeys.push(...keys);
                }
            }

            const redisUserIds = new Set<string>(
                presenceKeys.map(key => key.replace("presence:", ""))
            );

            // 3. Gom danh sách tất cả users cần đối soát (từ cả socket lẫn Redis)
            const allCandidateUserIds = new Set<string>([
                ...activeSocketUserIds,
                ...redisUserIds
            ]);

            if (allCandidateUserIds.size === 0) {
                return;
            }

            let fixedToOfflineCount = 0;
            let fixedToOnlineCount = 0;

            for (const userId of allCandidateUserIds) {
                const hasActiveSocket = activeSocketUserIds.has(userId);

                // Kiểm tra trạng thái hiện tại trong Redis
                const rawPresence = await redisClient.get(`presence:${userId}`);
                const isRedisOnline = rawPresence === "online";

                // Trường hợp 1: Socket đã ngắt (0 socket), nhưng Redis vẫn ghi nhận "online" (Zombie Presence)
                if (!hasActiveSocket && isRedisOnline) {
                    const lastActive = new Date();

                    // Cập nhật Redis thành offline
                    await redisClient.setJSON(`presence:${userId}`, {
                        status: "offline",
                        last_active: lastActive
                    }, 24 * 3600);

                    // Cập nhật DB via Facade
                    await userFacade.updatePresence(userId, lastActive);

                    // Bắn socket thông báo cho các user khác
                    io.to(ALL_USERS_ROOM).emit("user_offline", {
                        userId,
                        last_active: lastActive
                    });

                    fixedToOfflineCount++;
                }
                // Trường hợp 2: Socket đang kết nối (có socket), nhưng Redis lại là offline hoặc thiếu key
                else if (hasActiveSocket && !isRedisOnline) {
                    // Cập nhật Redis thành online
                    await redisClient.set(`presence:${userId}`, "online", 24 * 3600);

                    // Bắn socket thông báo cho các user khác
                    io.to(ALL_USERS_ROOM).emit("user_online", { userId });

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
