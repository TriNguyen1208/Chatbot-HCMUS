import { describe, it, expect, vi, beforeEach } from "vitest";
import { MediaController } from "../../media.controller.js";
import type { MediaService } from "../../media.service.js";
import type { Request, Response } from "express";

import { apiResponse } from "#@/shared/utils/api-response.util.js";

// Mock API Response
vi.mock("#@/shared/utils/api-response.util.js", () => ({
    apiResponse: {
        success: vi.fn((res, data) => {
            res.status(200).json({ success: true, data });
            return res;
        })
    }
}));

// Mock module QueueService
import { queueService } from "#@/background/queue.service.js";
vi.mock("#@/background/queue.service.js", () => ({
    queueService: {
        addJob: vi.fn()
    }
}));

import { checkSystemLoad } from "#@/shared/utils/system-monitor.util.js";
vi.mock("#@/shared/utils/system-monitor.util.js", () => ({
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
            handleImageUpload: vi.fn(),
            processAndUploadImage: vi.fn()
        } as unknown as MediaService;

        mediaController = new MediaController(mockMediaService);

        mockRes = {
            status: vi.fn().mockReturnThis(),
            json: vi.fn()
        };
    });

    it("should process image synchronously if system is not overloaded", async () => {
        vi.mocked(mockMediaService.handleImageUpload).mockResolvedValue({
            status: 'completed',
            url: "https://fakeurl.com/image.jpg"
        });

        mockReq = {
            user: { userID: "user_1" } as any,
            file: {
                buffer: Buffer.from("fake_image_data"),
                originalname: "test.jpg",
                mimetype: "image/jpeg"
            } as any
        };

        await mediaController.uploadImage(mockReq as Request, mockRes as Response, vi.fn());

        expect(mockMediaService.handleImageUpload).toHaveBeenCalled();
        expect(apiResponse.success).toHaveBeenCalledWith(mockRes, { resource_url: "https://fakeurl.com/image.jpg" });
    });

    it("should push image to queue and return 202 if system is overloaded", async () => {
        vi.mocked(mockMediaService.handleImageUpload).mockResolvedValue({
            status: 'queued',
            message: "The system is busy, images are being processed in the background."
        });

        mockReq = {
            user: { userID: "user_1" } as any,
            file: {
                buffer: Buffer.from("fake_image_data"),
                originalname: "test.jpg",
                mimetype: "image/jpeg"
            } as any
        };

        await mediaController.uploadImage(mockReq as Request, mockRes as Response, vi.fn());

        expect(mockMediaService.handleImageUpload).toHaveBeenCalled();
        expect(apiResponse.success).toHaveBeenCalledWith(mockRes, {}, {
            message: "The system is busy, images are being processed in the background."
        });
    });
});
