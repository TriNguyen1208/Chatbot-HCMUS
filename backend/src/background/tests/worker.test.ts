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
import { mediaFacade } from "#@/modules/media/media.facade.js";
vi.mock("#@/modules/media/media.facade.js", () => ({
    mediaFacade: {
        uploadImage: vi.fn().mockResolvedValue("https://fake.url/img.webp")
    }
}));

import { conversationFacade } from "#@/modules/conversation/conversation.facade.js";
vi.mock("#@/modules/conversation/conversation.facade.js", () => ({
    conversationFacade: {
        getConversation: vi.fn().mockResolvedValue(["u1", "u2"])
    }
}));

import { messageFacade } from "#@/modules/message/message.facade.js";
vi.mock("#@/modules/message/message.facade.js", () => ({
    messageFacade: {
        createMessageFromQueue: vi.fn().mockResolvedValue({ id: "msg-1", content: "hello" })
    }
}));

import { socketManager } from "#@/infrastructure/websocket/socket.manager.js";
vi.mock("#@/infrastructure/websocket/socket.manager.js", () => ({
    socketManager: {
        emitToUser: vi.fn(),
        emitToGroup: vi.fn(),
        emitToUsers: vi.fn()
    }
}));

import { fastWorker, processFastJob } from "../workers/fast.worker.js";
import { mediaWorker, processMediaJob } from "../workers/media.worker.js";

describe("Background Workers", () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it("should process 'upload_image' job successfully in media worker", async () => {
        const fakeJob = {
            id: "job-1",
            name: "upload_image",
            data: {
                userID: "u1",
                file_buffer: Buffer.from("fake-data"),
                file_name: "test.png",
                mime_type: "image/png"
            }
        };

        await processMediaJob(fakeJob as any);

        expect(mediaFacade.uploadImage).toHaveBeenCalledWith(
            expect.any(Buffer),
            "test.png",
            "image/png"
        );
        expect(socketManager.emitToUser).toHaveBeenCalledWith(
            "u1",
            "image_uploaded_success",
            { resource_url: "https://fake.url/img.webp" }
        );
    });

    it("should process 'create_message' job successfully in fast worker", async () => {
        const fakeJob = {
            id: "job-2",
            name: "create_message",
            data: {
                conversation_id: "c1",
                content: "hello"
            }
        };

        await processFastJob(fakeJob as any);

        expect(messageFacade.createMessageFromQueue).toHaveBeenCalledWith(fakeJob.data);
        expect(conversationFacade.getConversation).toHaveBeenCalledWith("c1");
        expect(socketManager.emitToUsers).toHaveBeenCalledWith(
            ["u1", "u2"],
            "new_message",
            { id: "msg-1", content: "hello" }
        );
    });
});
