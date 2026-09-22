import { config } from "#@/config/config.js";
import { mongoDB } from "#@/infrastructure/database/mongodb.connection.js";
import { initializeDatabaseModels } from "#@/infrastructure/database/init-models.js";
import { redisClient } from "#@/infrastructure/redis/redis.client.js";
import { checkElasticsearchConnection } from "#@/infrastructure/elasticsearch/es.client.js";
import { initializeIndices } from "#@/infrastructure/elasticsearch/es.indices.js";
import { initWorkerSocket } from "#@/infrastructure/websocket/socket.manager.js";
import { queueService } from "#@/background/queue.service.js";
import { workers } from "./background/workers/index.js";

const startWorker = async (): Promise<void> => {
    console.log("🚀 [Worker Process] Đang khởi động...");

    try {
        console.log("📦 [Worker Process] Đang kết nối MongoDB...");
        await mongoDB.connect();
        await initializeDatabaseModels();
        console.log("✅ [Worker Process] MongoDB đã sẵn sàng");

        console.log("🔴 [Worker Process] Đang kết nối Redis...");
        await redisClient.connect();
        console.log("✅ [Worker Process] Redis đã sẵn sàng");

        console.log("🔍 [Worker Process] Đang kiểm tra Elasticsearch...");
        await checkElasticsearchConnection();
        await initializeIndices();
        console.log("✅ [Worker Process] Elasticsearch đã sẵn sàng");

        initWorkerSocket();
        console.log("⚡ [Worker Process] Socket Emitter đã sẵn sàng");

        await queueService.initCronJobs();
        console.log("⏱️ [Worker Process] Đã kích hoạt Cron Jobs thành công");

        console.log("🎯 [Worker Process] Tất cả Workers & Cron Jobs đã sẵn sàng nhận việc!");
    } catch (error) {
        console.error("❌ [Worker Process] Khởi động thất bại:", error);
        process.exit(1);
    }
};

// Handle graceful shutdown
const handleShutdown = async (signal: string) => {
    console.log(`\n🛑 [Worker Process] Nhận tín hiệu ${signal}. Đang đóng các workers...`);
    try {
        await Promise.all(workers.map(w => w.close()));
        console.log("✅ [Worker Process] Đã đóng toàn bộ workers an toàn.");
    } catch (err) {
        console.error("❌ [Worker Process] Lỗi khi đóng workers:", err);
    } finally {
        process.exit(0);
    }
};

process.on("SIGTERM", () => handleShutdown("SIGTERM"));
process.on("SIGINT", () => handleShutdown("SIGINT"));

await startWorker();
