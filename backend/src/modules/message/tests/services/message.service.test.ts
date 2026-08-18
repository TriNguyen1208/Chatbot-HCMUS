import { describe, it, expect, vi, beforeEach } from "vitest";
import { MessageService } from "../../services/message.service.js";
import type { ConversationFacade } from "../../../conversation/conversation.facade.js";
import type { MessageRepository } from "../../repositories/message.repository.js";

// Mock module SocketManager
import { socketManager } from "#@/infrastructure/websocket/socket-manager.js";
vi.mock("#@/infrastructure/websocket/socket-manager.js", () => ({
    socketManager: {
        emitToGroup: vi.fn()
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

describe("MessageService", () => {
    let mockConversationFacade: ConversationFacade;
    let mockMessageRepo: MessageRepository;
    let messageService: MessageService;

    beforeEach(() => {
        vi.clearAllMocks();

        mockConversationFacade = {
            isUserInConversation: vi.fn()
        } as unknown as ConversationFacade;

        mockMessageRepo = {
            create: vi.fn()
        } as unknown as MessageRepository;

        messageService = new MessageService(mockConversationFacade, mockMessageRepo);
    });

    it("should throw Forbidden if user is not in conversation", async () => {
        vi.mocked(mockConversationFacade.isUserInConversation).mockResolvedValue(false);

        const payload = { conversation_id: "c1", type: "text" as const };
        await expect(messageService.handleIncomingMessage("u1", payload)).rejects.toThrow("You are not a member of this conversation");
    });

    it("should save to DB and emit socket when system load is normal", async () => {
        // ARRANGE
        vi.mocked(mockConversationFacade.isUserInConversation).mockResolvedValue(true);
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

    it("should push to queue and NOT emit socket when system is overloaded", async () => {
        // ARRANGE
        vi.mocked(mockConversationFacade.isUserInConversation).mockResolvedValue(true);
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
