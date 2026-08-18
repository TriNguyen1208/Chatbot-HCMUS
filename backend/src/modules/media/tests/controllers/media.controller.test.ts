import { describe, it, expect, vi, beforeEach } from "vitest";
import { MediaController } from "../../controllers/media.controller.js";
import type { MediaService } from "../../services/media.service.js";
import type { Request, Response } from "express";

// Mock API Response
vi.mock("#@/shared/utils/api-response.js", () => ({
    apiResponse: {
        success: vi.fn((res, data) => {
            res.status(200).json({ success: true, data });
            return res;
        })
    }
}));

// Mock module QueueContainer
import { queueService } from "#@/modules/queue/queue.container.js";
vi.mock("#@/modules/queue/queue.container.js", () => ({
    queueService: {
        addJob: vi.fn()
    }
}));

// Mock system-monitor
import { checkSystemLoad } from "#@/shared/utils/system-monitor.js";
vi.mock("#@/shared/utils/system-monitor.js", () => ({
    checkSystemLoad: vi.fn()
}));

describe("MediaController", () => {
    let mockMediaService: MediaService;
    let mediaController: MediaController;
    let mockReq: Partial<Request>;
    let mockRes: Partial<Response>;

    beforeEach(() => {
        vi.clearAllMocks();

        mockMediaService = {
            processAndUploadImage: vi.fn(),
            createCloudflareUploadURL: vi.fn(),
            handleCloudflareWebhook: vi.fn()
        } as unknown as MediaService;

        mediaController = new MediaController(mockMediaService);

        mockRes = {
            status: vi.fn().mockReturnThis(),
            json: vi.fn()
        };
    });

    it("should process image synchronously if system is not overloaded", async () => {
        vi.mocked(checkSystemLoad).mockResolvedValue(false);
        vi.mocked(mockMediaService.processAndUploadImage).mockResolvedValue("https://fakeurl.com/image.jpg");

        mockReq = {
            user: { userID: "user_1" } as any,
            file: {
                buffer: Buffer.from("fake_image_data"),
                originalname: "test.jpg",
                mimetype: "image/jpeg"
            } as any
        };

        await mediaController.uploadImage(mockReq as Request, mockRes as Response, vi.fn());

        expect(mockMediaService.processAndUploadImage).toHaveBeenCalled();
        expect(queueService.addJob).not.toHaveBeenCalled();
        expect(mockRes.status).toHaveBeenCalledWith(200);
        expect(mockRes.json).toHaveBeenCalledWith({
            success: true,
            data: { resource_url: "https://fakeurl.com/image.jpg" }
        });
    });

    it("should push image to queue and return 202 if system is overloaded", async () => {
        vi.mocked(checkSystemLoad).mockResolvedValue(true);

        mockReq = {
            user: { userID: "user_1" } as any,
            file: {
                buffer: Buffer.from("fake_image_data"),
                originalname: "test.jpg",
                mimetype: "image/jpeg"
            } as any
        };

        await mediaController.uploadImage(mockReq as Request, mockRes as Response, vi.fn());

        expect(queueService.addJob).toHaveBeenCalledWith('upload_image', expect.objectContaining({
            userID: "user_1",
            file_name: "test.jpg",
            mime_type: "image/jpeg"
        }));
        
        expect(mockMediaService.processAndUploadImage).not.toHaveBeenCalled();
        
        expect(mockRes.status).toHaveBeenCalledWith(202);
        expect(mockRes.json).toHaveBeenCalledWith({
            success: true,
            message: "The system is busy, images are being processed in the background."
        });
    });

    describe("Video Endpoints", () => {
        it("should return stream upload url successfully", async () => {
            const mockUploadUrlData = { uploadURL: "https://cloudflare.com/upload", uid: "video-123" };
            vi.mocked(mockMediaService.createCloudflareUploadURL).mockResolvedValue(mockUploadUrlData as any);

            await mediaController.getStreamUploadUrl({} as Request, mockRes as Response, vi.fn());

            expect(mockMediaService.createCloudflareUploadURL).toHaveBeenCalled();
            expect(mockRes.status).toHaveBeenCalledWith(200);
            expect(mockRes.json).toHaveBeenCalledWith({
                success: true,
                data: mockUploadUrlData
            });
        });

        it("should handle Cloudflare Webhook and return 200 immediately", async () => {
            const mockReqWebhook = {
                headers: { 'webhook-signature': 'fake-signature' },
                body: { text: "video_processing_done" }
            };
            
            const mockSend = vi.fn();
            mockRes.status = vi.fn().mockReturnValue({ send: mockSend });
            
            // Simulate background processing (runs in the background but cannot block requests)
            vi.mocked(mockMediaService.handleCloudflareWebhook).mockResolvedValue(undefined);

            await mediaController.handleCloudflareWebhook(mockReqWebhook as any, mockRes as Response, vi.fn());

            // Guaranteed to return 200 OK immediately
            expect(mockRes.status).toHaveBeenCalledWith(200);
            expect(mockSend).toHaveBeenCalledWith('OK');

            // Make sure the webhook function is actually called
            expect(mockMediaService.handleCloudflareWebhook).toHaveBeenCalledWith(mockReqWebhook.body, 'fake-signature');
        });
    });
});
