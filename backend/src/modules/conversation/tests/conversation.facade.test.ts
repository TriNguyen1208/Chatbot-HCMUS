import { describe, it, expect, vi } from "vitest";
import { ConversationFacade } from "../conversation.facade.js";
import type { IConversationRepository } from "../repositories/conversation.repository.js";

describe("ConversationFacade", () => {
    it("should correctly verify if a user is in a conversation", async () => {
        // 1. Create a Mock (fake copy) of the Repository
        // Thanks to the application of Interface, we can easily create fake objects without connecting to the real Database
        const mockRepo: IConversationRepository = {
            create: vi.fn(),
            findByID: vi.fn(),
            getConversationsByUser: vi.fn(),
            findDirectConversation: vi.fn(),
            addMembers: vi.fn(),
            removeMember: vi.fn(),
            updateLastMessage: vi.fn(),
            // Simulate this function will always return true when receiving the corresponding parameter
            checkUserInConversation: vi.fn().mockResolvedValue(true)
        };

        const mockService = {} as any;
        // 2. Initialize Facade and embed Mock Repo (Dependency Injection)
        const facade = new ConversationFacade(mockRepo, mockService);

        // 3. Execute action
        const result = await facade.isUserInConversation("conv-123", "user-abc");

        // 4. Check the results
        // Expect the return result to be exactly like the programmed Mock version (true)
        expect(result).toBe(true);
        
        // Expect the function in the Repo to have been called exactly once with the correct parameters
        expect(mockRepo.checkUserInConversation).toHaveBeenCalledWith("conv-123", "user-abc");
        expect(mockRepo.checkUserInConversation).toHaveBeenCalledTimes(1);
    });
});
