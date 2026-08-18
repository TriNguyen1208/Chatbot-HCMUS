import { describe, it, expect, vi, beforeEach } from "vitest";
import { ConversationController } from "../controllers/conversation.controller.js";
import { ConversationService } from "../services/conversation.service.js";
import { apiResponse } from "#@/shared/utils/api-response.js";
import type { Request, Response } from "express";

vi.mock("#@/shared/utils/api-response.js", () => ({
    apiResponse: {
        success: vi.fn((res) => res),
        error: vi.fn((res) => res)
    }
}));

describe("ConversationController", () => {
    let controller: ConversationController;
    let mockService: Partial<ConversationService>;
    let mockReq: Partial<Request>;
    let mockRes: Partial<Response>;

    beforeEach(() => {
        mockService = {
            createConversation: vi.fn(),
            getConversationById: vi.fn(),
            getConversationList: vi.fn(),
            addMember: vi.fn(),
            removeMember: vi.fn(),
            leaveGroup: vi.fn(),
        };

        controller = new ConversationController(mockService as ConversationService);

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
        
        vi.clearAllMocks();
    });

    describe("createConversation", () => {
        it("should call service and return success response", async () => {
            mockReq.body = { type: "group", member_ids: ["user_2"], name: "Group" };
            const mockResponse = { _id: "conv_1", ...mockReq.body };
            (mockService.createConversation as any).mockResolvedValue(mockResponse);

            await controller.createConversation(mockReq as Request, mockRes as Response);

            expect(mockService.createConversation).toHaveBeenCalledWith("user_1", mockReq.body);
            expect(apiResponse.success).toHaveBeenCalledWith(mockRes, mockResponse, expect.objectContaining({
                statusCode: 201
            }));
        });
    });

    describe("getConversation", () => {
        it("should call service and return success response", async () => {
            mockReq.params = { id: "conv_1" };
            const mockResponse = { _id: "conv_1", name: "Group" };
            (mockService.getConversationById as any).mockResolvedValue(mockResponse);

            await controller.getConversation(mockReq as Request, mockRes as Response);

            expect(mockService.getConversationById).toHaveBeenCalledWith("conv_1", "user_1");
            expect(apiResponse.success).toHaveBeenCalledWith(mockRes, mockResponse, expect.objectContaining({
                statusCode: 200
            }));
        });
    });

    describe("getList", () => {
        it("should call service and return success response", async () => {
            const dateStr = new Date().toISOString();
            mockReq.query = { limit: "20", cursor_date: dateStr, cursor_id: "conv_last" };
            const mockResponse = { data: [], metadata: { hasNextPage: false } };
            (mockService.getConversationList as any).mockResolvedValue(mockResponse);

            await controller.getList(mockReq as Request, mockRes as Response);

            expect(mockService.getConversationList).toHaveBeenCalledWith(
                "user_1", 
                "20", 
                dateStr, 
                "conv_last"
            );
            expect(apiResponse.success).toHaveBeenCalledWith(mockRes, mockResponse, expect.objectContaining({
                statusCode: 200
            }));
        });
    });

    describe("addMember", () => {
        it("should call service and return success response", async () => {
            mockReq.params = { id: "conv_1" };
            mockReq.body = { member_ids: ["user_2", "user_3"] };

            await controller.addMember(mockReq as Request, mockRes as Response);

            expect(mockService.addMember).toHaveBeenCalledWith("user_1", "conv_1", ["user_2", "user_3"]);
            expect(apiResponse.success).toHaveBeenCalledWith(mockRes, null, expect.objectContaining({
                statusCode: 200
            }));
        });
    });

    describe("removeMember", () => {
        it("should call service and return success response", async () => {
            mockReq.params = { id: "conv_1" };
            mockReq.body = { member_id: "user_2" };

            await controller.removeMember(mockReq as Request, mockRes as Response);

            expect(mockService.removeMember).toHaveBeenCalledWith("user_1", "conv_1", "user_2");
            expect(apiResponse.success).toHaveBeenCalledWith(mockRes, null, expect.objectContaining({
                statusCode: 200
            }));
        });
    });

    describe("leaveGroup", () => {
        it("should call service and return success response", async () => {
            mockReq.params = { id: "conv_1" };

            await controller.leaveGroup(mockReq as Request, mockRes as Response);

            expect(mockService.leaveGroup).toHaveBeenCalledWith("user_1", "conv_1");
            expect(apiResponse.success).toHaveBeenCalledWith(mockRes, null, expect.objectContaining({
                statusCode: 200
            }));
        });
    });
});
