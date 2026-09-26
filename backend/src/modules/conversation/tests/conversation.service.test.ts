import { describe, it, expect, vi, beforeEach } from "vitest";
import { ConversationService } from "../conversation.service.js";
import type { IConversationRepository } from "../conversation.repository.js";
import type { ConversationCache } from "../conversation.cache.js";
import type { MessageFacade } from "#@/modules/message/message.facade.js";

// Mock socketManager
import { socketManager } from "#@/infrastructure/websocket/socket.manager.js";
vi.mock("#@/infrastructure/websocket/socket.manager.js", () => ({
    socketManager: {
        emitToGroup: vi.fn(),
        emitToUsers: vi.fn(),
        joinGroup: vi.fn(),
        leaveGroup: vi.fn()
    }
}));

// Mock userFacade
import { userFacade } from "#@/modules/user/user.facade.js";
vi.mock("#@/modules/user/user.facade.js", () => ({
    userFacade: {
        findByID: vi.fn().mockResolvedValue({ id: "u1", avatar_url: "http://avatar.com/u1.png" })
    }
}));

// Mock triggerSync
vi.mock("#@/shared/utils/sync.util.js", () => ({
    triggerSync: vi.fn(),
    SyncOperation: { CREATE: 'CREATE', UPDATE: 'UPDATE', DELETE: 'DELETE' }
}));

describe("ConversationService - Idempotency", () => {
    let mockRepo: IConversationRepository;
    let mockCache: ConversationCache;
    let mockMessageFacade: MessageFacade;

    beforeEach(() => {
        vi.clearAllMocks();

        mockRepo = {
            create: vi.fn(),
            findDirectConversation: vi.fn(),
            findSelfConversation: vi.fn(),
            findById: vi.fn(),
        } as unknown as IConversationRepository;

        mockCache = {
            setConversation: vi.fn().mockResolvedValue(undefined),
            addUserConv: vi.fn().mockResolvedValue(undefined),
            setDirectConvId: vi.fn().mockResolvedValue(undefined),
            setSelfConvId: vi.fn().mockResolvedValue(undefined),
            getDirectConvId: vi.fn().mockResolvedValue(null),
            getSelfConvId: vi.fn().mockResolvedValue(null),
        } as unknown as ConversationCache;

        mockMessageFacade = {
            handleIncomingMessage: vi.fn().mockResolvedValue({ status: 'success' }),
            createSystemMessage: vi.fn().mockResolvedValue({ id: 'sys-1' })
        } as unknown as MessageFacade;
    });

    it("should prevent duplicate group creation when idempotency_key is reused", async () => {
        const mockCreatedGroup = {
            id: "group-100",
            name: "Học Tập",
            type: "group" as const,
            member_ids: ["u1", "u2", "u3"],
            admin_ids: ["u1"]
        };

        vi.mocked(mockRepo.create).mockResolvedValue(mockCreatedGroup as any);

        const mockIdempotency = {
            execute: vi.fn()
                .mockImplementationOnce(async (_key: string, _ttl: number, action: () => Promise<any>) => ({
                    isDuplicate: false,
                    data: await action()
                }))
                .mockImplementationOnce(async () => ({
                    isDuplicate: true,
                    data: mockCreatedGroup
                }))
        } as any;

        const conversationService = new ConversationService(
            mockRepo,
            mockCache,
            mockMessageFacade,
            mockIdempotency
        );

        const groupDto = {
            name: "Học Tập",
            type: "group" as const,
            member_ids: ["u2", "u3"],
            primary_icon: "👍",
            idempotency_key: "group-uuid-unique-1"
        };

        // ACT 1: Tạo nhóm lần 1
        const res1 = await conversationService.createConversation("u1", groupDto);
        expect(res1.id).toBe("group-100");
        expect(mockRepo.create).toHaveBeenCalledTimes(1);
        expect(socketManager.emitToUsers).toHaveBeenCalledTimes(1);
        expect(socketManager.emitToGroup).toHaveBeenCalledTimes(1);

        // ACT 2: Client gửi lại (do network retry timeout) cùng idempotency_key
        const res2 = await conversationService.createConversation("u1", groupDto);
        expect(res2.id).toBe("group-100");
        // MongoDB create và socket emits KHÔNG được gọi lần 2
        expect(mockRepo.create).toHaveBeenCalledTimes(1);
        expect(socketManager.emitToUsers).toHaveBeenCalledTimes(1);
        expect(socketManager.emitToGroup).toHaveBeenCalledTimes(1);
    });
});
