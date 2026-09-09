import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock BullMQ to not actually connect to Redis when running the test
vi.mock("bullmq", () => {
    return {
        Worker: class {
            processor: Function;
            constructor(name: string, processor: Function) {
                this.processor = processor; // Expose the processor for the test to call directly
            }
            on = vi.fn();
        },
        Queue: class {
            add = vi.fn();
        }
    };
});

// Mock dependencies
import { mediaContainer } from "#@/modules/media/media.container.js";
vi.mock("#@/modules/media/media.container.js", () => ({
    mediaContainer: {
        mediaService: {
            processAndUploadImage: vi.fn()
        }
    }
}));

import { conversationFacade } from "#@/modules/conversation/conversation.facade.js";
vi.mock("#@/modules/conversation/conversation.facade.js", () => ({
    conversationFacade: {
        getConversation: vi.fn().mockResolvedValue(["u1", "u2"])
    }
}));

import { messageContainer } from "#@/modules/message/message.container.js";
vi.mock("#@/modules/message/message.container.js", () => ({
    messageContainer: {
        messageRepo: { create: vi.fn() }
    }
}));

import { socketManager } from "#@/infrastructure/websocket/socket-manager.js";
vi.mock("#@/infrastructure/websocket/socket-manager.js", () => ({
    socketManager: {
        emitToUser: vi.fn(),
        emitToGroup: vi.fn(),
        emitToUsers: vi.fn()
    }
}));

import worker from "../queue.worker.js";

describe("QueueWorker", () => {
    let processorFn: Function;

    beforeEach(() => {
        vi.clearAllMocks();
        processorFn = (worker as any).processor;
    });

    it("should process 'upload_image' job successfully", async () => {
        vi.mocked(mediaContainer.mediaService.processAndUploadImage).mockResolvedValue("https://fakeurl.com/anh.jpg" as any);

        const mockJob = {
            name: 'upload_image',
            id: 'job-1',
            data: {
                userID: "u1",
                file_buffer: { data: [1, 2, 3] },
                file_name: "test.jpg",
                mime_type: "image/jpeg"
            }
        };

        await processorFn(mockJob);

        expect(mediaContainer.mediaService.processAndUploadImage).toHaveBeenCalled();
        expect(socketManager.emitToUser).toHaveBeenCalledWith("u1", "image_uploaded_success", {
            resource_url: "https://fakeurl.com/anh.jpg"
        });
    });

    it("should process 'create_message' job successfully", async () => {
        const mockSavedMessage = { id: "msg-123", content: "Hello" };
        vi.mocked(messageContainer.messageRepo.create).mockResolvedValue(mockSavedMessage as any);

        const mockJob = {
            name: 'create_message',
            id: 'job-2',
            data: {
                conversation_id: "c1",
                content: "Hello",
                type: "text"
            }
        };

        await processorFn(mockJob);

        expect(messageContainer.messageRepo.create).toHaveBeenCalledWith(mockJob.data);
        expect(socketManager.emitToUsers).toHaveBeenCalledWith(["u1", "u2"], "new_message", mockSavedMessage);
    });

    it("should process cron jobs successfully", async () => {
        const consoleLogSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

        const mockJob = {
            name: 'cleanup_orphaned_media',
            id: 'cron_cleanup_orphaned_media',
            data: {}
        };

        await processorFn(mockJob);

        expect(consoleLogSpy).toHaveBeenCalledWith(expect.stringContaining("Bắt đầu quét rác Cloudflare R2"));
        consoleLogSpy.mockRestore();
    });

    it("should log warning if job name is unknown", async () => {
        const consoleWarnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
        
        const mockJob = {
            name: 'unknown_job',
            id: 'job-3',
            data: {}
        };

        await processorFn(mockJob);

        expect(consoleWarnSpy).toHaveBeenCalledWith(expect.stringContaining("No handler found for type Job: unknown_job"));
        consoleWarnSpy.mockRestore();
    });
});
