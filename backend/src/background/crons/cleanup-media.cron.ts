import type { CronJobDefinition } from "../queue.types.js";

export const cleanupMediaCron: CronJobDefinition = {
    name: "cleanup_media",
    cronExpression: "0 3 * * *", // 03:00 sáng hàng ngày
    handler: async () => {
        console.log("🧹 [Cron:cleanup_media] Bắt đầu quét rác Cloudflare R2...");
        
        // TODO: Triển khai logic:
        // 1. Quét danh sách các file keys trên Cloudflare R2
        // 2. Đối chiếu với MongoDB xem file nào không thuộc message hoặc user avatar
        // 3. Xoá các file mồ côi (orphaned files) khỏi Cloudflare R2
        
        console.log("✅ [Cron:cleanup_media] Hoàn tất dọn dẹp media mồ côi.");
    },
    options: {
        attempts: 2,
        removeOnComplete: 10,
        removeOnFail: 50
    }
};
