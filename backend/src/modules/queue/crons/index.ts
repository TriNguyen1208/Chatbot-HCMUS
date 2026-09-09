import type { CronJobDefinition } from "../queue.types.js";
import { cleanupOrphanedMediaCron } from "./cleanup-orphaned-media.cron.js";
import { syncPresenceCron } from "./sync-presence.cron.js";

/**
 * Danh sách toàn bộ Cron Jobs được đăng ký trong hệ thống.
 * Muốn thêm một Cron Job mới, chỉ cần tạo file trong thư mục crons/ và import vào đây.
 */
export const registeredCrons: CronJobDefinition[] = [
    cleanupOrphanedMediaCron,
    syncPresenceCron
];

/**
 * Map tra cứu nhanh handler tương ứng theo tên job
 */
export const cronHandlerMap = new Map<string, () => Promise<void>>(
    registeredCrons.map((cron) => [cron.name, cron.handler])
);
