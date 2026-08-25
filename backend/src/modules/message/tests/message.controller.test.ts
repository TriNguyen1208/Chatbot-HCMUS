import { describe, it, expect, vi, beforeEach } from "vitest";
import { MessageController } from "../controllers/message.controller.js";
import { MessageService } from "../services/message.service.js";
import { apiResponse } from "#@/shared/utils/api-response.js";
import type { Request, Response, NextFunction } from "express";

vi.mock("#@/shared/utils/api-response.js", () => ({
    apiResponse: {
        success: vi.fn((res) => res),
        error: vi.fn((res) => res)
    }
}));

describe("MessageController", () => {
    let controller: MessageController;
    let mockService: Partial<MessageService>;
    let mockReq: Partial<Request>;
    let mockRes: Partial<Response>;
    let mockNext: NextFunction;

    beforeEach(() => {
        mockService = {
            handleIncomingMessage: vi.fn(),
            getMessages: vi.fn(),
            editMessage: vi.fn(),
            recallMessage: vi.fn(),
        };

        controller = new MessageController(mockService as MessageService);

        mockReq = {
            user: { userID: "user_1" } as any,
            body: {},
            params: {},
            query: {}
        };

        mockRes = {
            status: vi.fn().mockReturnThis(),
            json: vi.fn().mockReturnThis(),
        };

        mockNext = vi.fn();
        
        vi.clearAllMocks();
    });

    describe("sendMessage", () => {
        it("should call service and return success response", async () => {
            const mockPayload = { conversation_id: "conv_1", content: "Hello", type: "text" } as any;
            mockReq.body = mockPayload;
            
            const mockResult = { id: "msg_1", ...mockPayload };
            (mockService.handleIncomingMessage as any).mockResolvedValue(mockResult);

            await controller.sendMessage(mockReq as Request, mockRes as Response, mockNext);

            expect(mockService.handleIncomingMessage).toHaveBeenCalledWith("user_1", mockPayload);
            expect(apiResponse.success).toHaveBeenCalledWith(mockRes, mockResult);
        });
    });

    describe("getMessages", () => {
        it("should call service and return success response", async () => {
            const dateStr = new Date().toISOString();
            mockReq.params = { conversation_id: "conv_1" };
            mockReq.query = { limit: "20", cursor_date: dateStr, cursor_id: "msg_last" };
            
            const mockMessages = { data: [], metadata: { hasNextPage: false } };
            (mockService.getMessages as any).mockResolvedValue(mockMessages);

            await controller.getMessages(mockReq as Request, mockRes as Response, mockNext);

            expect(mockService.getMessages).toHaveBeenCalledWith(
                "conv_1", 
                "user_1", 
                "20", 
                dateStr, 
                "msg_last"
            );
            expect(apiResponse.success).toHaveBeenCalledWith(mockRes, mockMessages);
        });
    });

    describe("editMessage", () => {
        it("should call service and return success response", async () => {
            mockReq.params = { id: "msg_1" };
            mockReq.body = { content: "Edited text" };

            await controller.editMessage(mockReq as Request, mockRes as Response, mockNext);

            expect(mockService.editMessage).toHaveBeenCalledWith("msg_1", "user_1", "Edited text");
            expect(apiResponse.success).toHaveBeenCalledWith(mockRes, null, expect.objectContaining({
                message: "Message edited successfully"
            }));
        });
    });

    describe("recallMessage", () => {
        it("should call service and return success response", async () => {
            mockReq.params = { id: "msg_1" };

            await controller.recallMessage(mockReq as Request, mockRes as Response, mockNext);

            expect(mockService.recallMessage).toHaveBeenCalledWith("msg_1", "user_1");
            expect(apiResponse.success).toHaveBeenCalledWith(mockRes, null, expect.objectContaining({
                message: "Message recalled successfully"
            }));
        });
    });
});
