import { describe, it, expect, vi, beforeEach } from "vitest";
import { MessageService } from "../../message.service.js";
import type { ConversationFacade } from "#@/modules/conversation/conversation.facade.js";
import type { MessageRepository } from "../../message.repository.js";

// Mock module SocketManager
import { socketManager } from "#@/infrastructure/websocket/socket.manager.js";
vi.mock("#@/infrastructure/websocket/socket.manager.js", () => ({
    socketManager: {
        emitToGroup: vi.fn(),
        emitToUsers: vi.fn()
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

        messageService = new MessageService(mockConversationFacade, mockMessageRepo);
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
        expect(socketManager.emitToUsers).toHaveBeenCalledWith(["u1", "u2"], "new_message", mockSavedMessage);
        expect(queueService.addJob).not.toHaveBeenCalled();
    });

    it("should push to queue and NOT emit socket when system is overloaded", async () => {
        // ARRANGE
        vi.mocked(mockConversationFacade.getConversationById).mockResolvedValue({ id: "c1", is_active: true } as any);
        vi.mocked(checkSystemLoad).mockResolvedValue(true); // System is busy
        
        const payload = { conversation_id: "c1", content: "Test", type: "text" as const };

        // ACT
        const result = await messageService.handleIncomingMessage("u1", payload);

        // ASSERT
        expect(result.status).toBe('queued');
        expect(queueService.addJob).toHaveBeenCalledWith('create_message', expect.objectContaining({
            conversation_id: 'c1',
            content: 'Test'
        }));
        
        // Do not save DB and Do not fire socket
        expect(mockMessageRepo.create).not.toHaveBeenCalled();
        expect(socketManager.emitToGroup).not.toHaveBeenCalled();
    });
});
