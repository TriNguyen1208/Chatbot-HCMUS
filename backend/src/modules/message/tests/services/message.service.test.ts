import { describe, it, expect, vi, beforeEach } from "vitest";
import { MessageService } from "../../message.service.js";
import type { ConversationFacade } from "#@/modules/conversation/conversation.facade.js";
import type { MessageRepository } from "../../message.repository.js";
import type { MessageCache } from "../../message.cache.js";

// Mock module SocketManager
import { socketManager } from "#@/infrastructure/websocket/socket.manager.js";
vi.mock("#@/infrastructure/websocket/socket.manager.js", () => ({
    socketManager: {
        emitToGroup: vi.fn(),
        emitToUser: vi.fn(),
        joinGroup: vi.fn(),
        leaveGroup: vi.fn()
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

// Mock triggerSync
vi.mock("#@/shared/utils/sync.util.js", () => ({
    triggerSync: vi.fn(),
    SyncOperation: { CREATE: 'CREATE', UPDATE: 'UPDATE', DELETE: 'DELETE' }
}));

describe("MessageService", () => {
    let mockConversationFacade: ConversationFacade;
    let mockMessageRepo: MessageRepository;
    let mockMessageCache: MessageCache;
    let messageService: MessageService;

    beforeEach(() => {
        vi.clearAllMocks();

        mockConversationFacade = {
            getConversationById: vi.fn(),
            updateLastMessage: vi.fn().mockResolvedValue(undefined),
            getConversationMembers: vi.fn().mockResolvedValue(["u1", "u2"])
        } as unknown as ConversationFacade;

        mockMessageRepo = {
            create: vi.fn()
        } as unknown as MessageRepository;

        mockMessageCache = {
            pushRecent: vi.fn().mockResolvedValue(undefined),
            getRecent: vi.fn().mockResolvedValue(null),
            setRecent: vi.fn().mockResolvedValue(undefined),
            updateRecent: vi.fn().mockResolvedValue(undefined),
            clearRecent: vi.fn().mockResolvedValue(undefined)
        } as unknown as MessageCache;

        messageService = new MessageService(mockConversationFacade, mockMessageRepo, mockMessageCache);
    });

    it("should throw Forbidden if user is not in conversation", async () => {
        vi.mocked(mockConversationFacade.getConversationById).mockResolvedValue(null as any);

        const payload = { conversation_id: "c1", type: "text" as const };
        await expect(messageService.handleIncomingMessage("u1", payload)).rejects.toThrow("You are not a member of this conversation");
    });

    it("should save to DB and emit socket when system load is normal", async () => {
        // ARRANGE
        vi.mocked(mockConversationFacade.getConversationById).mockResolvedValue({ id: "c1", is_active: true } as any);
        vi.mocked(mockConversationFacade.getConversationMembers).mockResolvedValue(["u1", "u2"]);
        vi.mocked(checkSystemLoad).mockResolvedValue(false); // System free
        
        const mockSavedMessage = { id: "msg-123", content: "Test" };
        vi.mocked(mockMessageRepo.create).mockResolvedValue(mockSavedMessage as any);

        const payload = { conversation_id: "c1", content: "Test", type: "text" as const };

        // ACT
        const result = await messageService.handleIncomingMessage("u1", payload);

        // ASSERT
        expect(result.status).toBe('success');
        expect(mockMessageRepo.create).toHaveBeenCalled();
        expect(socketManager.emitToGroup).toHaveBeenCalledWith("c1", "new_message", mockSavedMessage);
        expect(queueService.addJob).not.toHaveBeenCalled();
    });

    it("should prevent duplicate message creation when client_msg_id is reused", async () => {
        vi.mocked(mockConversationFacade.getConversationById).mockResolvedValue({ id: "c1", is_active: true } as any);
        vi.mocked(mockConversationFacade.getConversationMembers).mockResolvedValue(["u1", "u2"]);
        
        const mockSavedMessage = { id: "msg-123", content: "Hello once" };
        vi.mocked(mockMessageRepo.create).mockResolvedValue(mockSavedMessage as any);

        const mockIdempotency = {
            execute: vi.fn()
                // Lần 1: Thực thi thành công
                .mockImplementationOnce(async (_key: string, _ttl: number, action: () => Promise<any>) => ({
                    isDuplicate: false,
                    data: await action()
                }))
                // Lần 2: Nhận diện duplicate và trả về data cached
                .mockImplementationOnce(async () => ({
                    isDuplicate: true,
                    data: { status: 'success', data: mockSavedMessage }
                }))
        } as any;

        const serviceWithIdempotency = new MessageService(
            mockConversationFacade,
            mockMessageRepo,
            mockMessageCache,
            mockIdempotency
        );

        const payload = { conversation_id: "c1", content: "Hello once", type: "text" as const, client_msg_id: "uuid-msg-1" };

        // ACT 1: Gửi lần đầu
        const res1 = await serviceWithIdempotency.handleIncomingMessage("u1", payload);
        expect(res1.status).toBe('success');
        expect(res1.data).toEqual(mockSavedMessage);
        expect(mockMessageRepo.create).toHaveBeenCalledTimes(1);
        expect(socketManager.emitToGroup).toHaveBeenCalledTimes(1);

        // ACT 2: Retry gửi cùng client_msg_id
        const res2 = await serviceWithIdempotency.handleIncomingMessage("u1", payload);
        expect(res2.status).toBe('success');
        expect(res2.data).toEqual(mockSavedMessage);
        // MongoDB create và emitToGroup KHÔNG được gọi thêm lần nào
        expect(mockMessageRepo.create).toHaveBeenCalledTimes(1);
        expect(socketManager.emitToGroup).toHaveBeenCalledTimes(1);
    });
});

