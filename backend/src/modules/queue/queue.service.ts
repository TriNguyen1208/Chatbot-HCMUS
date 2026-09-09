import { Queue } from "bullmq";
import { config } from "#@/config/config.js";
import { QueueName, type CronJobDefinition, type QueueJobOptions } from "./queue.types.js";
import { registeredCrons } from "./crons/index.js";

export class QueueService {
    // 3 Queues chuyên biệt theo đặc thù tác vụ
    public readonly fastQueue: Queue;
    public readonly mediaQueue: Queue;
    public readonly cronQueue: Queue;

    constructor() {
        const connection = {
            host: config.redis.host,
            port: config.redis.port,
            password: config.redis.password
        };

        // 1. Queue xử lý nhanh (Message, Sync DB/ES...)
        this.fastQueue = new Queue(QueueName.FAST, { connection });

        // 2. Queue xử lý tác vụ nặng CPU / I/O (Video FFmpeg, Media...)
        this.mediaQueue = new Queue(QueueName.MEDIA, { connection });

        // 3. Queue xử lý tác vụ định kỳ (Cron Jobs)
        this.cronQueue = new Queue(QueueName.CRON, { connection });
    }

    /**
     * Tự động xác định Queue đích phù hợp dựa trên tên của Job
     */
    private resolveQueue(jobName: string, targetQueue?: QueueName): Queue {
        if (targetQueue) {
            switch (targetQueue) {
                case QueueName.MEDIA: return this.mediaQueue;
                case QueueName.CRON: return this.cronQueue;
                case QueueName.FAST:
                default: return this.fastQueue;
            }
        }

        // Tự động phân luồng thông minh nếu không chỉ định rõ:
        if (['upload_image', 'process_video'].includes(jobName)) {
            return this.mediaQueue;
        }

        if (jobName.startsWith('cleanup_') || jobName.startsWith('cron_')) {
            return this.cronQueue;
        }

        return this.fastQueue;
    }

    /**
     * Thêm một Job tức thời (On-demand) vào hàng đợi tương ứng.
     * @param jobName Tên Job (VD: 'upload_image', 'create_message')
     * @param payload Dữ liệu truyền vào
     * @param customOptions Tuỳ chọn (priority, delay, attempts...)
     * @param targetQueue Chỉ định rõ Queue nếu không muốn tự động phân luồng
     */
    async addJob(jobName: string, payload: any, customOptions?: QueueJobOptions, targetQueue?: QueueName) {
        const defaultOptions: QueueJobOptions = {
            attempts: 3, // Thử lại 3 lần nếu có lỗi
            backoff: {
                type: "exponential",
                delay: 2000 // Chờ 2s, 4s, 8s giữa các lần retry
            },
            removeOnComplete: true // Tự động dọn dẹp job khi xong để tránh tốn RAM Redis
        };

        const queue = this.resolveQueue(jobName, targetQueue);

        return await queue.add(jobName, payload, {
            ...defaultOptions,
            ...customOptions
        });
    }

    /**
     * Đăng ký một Cron Job (Repeatable Job) vào cronQueue.
     * Sử dụng jobId cố định để đảm bảo tính Idempotent (không bị trùng lặp).
     */
    async addCronJob(cron: CronJobDefinition) {
        return await this.cronQueue.add(
            cron.name,
            {},
            {
                jobId: `cron_${cron.name}`, // Khóa ID duy nhất
                repeat: {
                    pattern: cron.cronExpression
                },
                attempts: cron.options?.attempts ?? 1,
                removeOnComplete: cron.options?.removeOnComplete ?? true,
                removeOnFail: cron.options?.removeOnFail ?? 50
            }
        );
    }

    /**
     * Tự động đăng ký toàn bộ Cron Jobs được khai báo trong crons/index.ts vào cronQueue
     */
    async initCronJobs(crons: CronJobDefinition[] = registeredCrons) {
        try {
            for (const cron of crons) {
                await this.addCronJob(cron);
            }
            console.log(`⏱️ [QueueService] Đã đồng bộ ${crons.length} cron jobs vào [${QueueName.CRON}].`);
        } catch (error) {
            console.error("❌ [QueueService] Lỗi khi khởi tạo Cron Jobs:", error);
        }
    }

    /**
     * Lấy instance Queue theo tên nếu cần can thiệp trực tiếp
     */
    getQueue(queueName: QueueName = QueueName.FAST): Queue {
        switch (queueName) {
            case QueueName.MEDIA: return this.mediaQueue;
            case QueueName.CRON: return this.cronQueue;
            case QueueName.FAST:
            default: return this.fastQueue;
        }
    }
}

export const queueService = new QueueService();