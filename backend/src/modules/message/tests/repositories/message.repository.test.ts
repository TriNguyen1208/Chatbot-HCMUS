import { describe, it, expect, vi, beforeEach } from "vitest";
import { MessageRepository } from "../../repositories/message.repository.js";
import type { IDatabase } from "#@/infrastructure/database/database.interface.js";
import type { Message } from "../../entities/message.entity.js";

describe("MessageRepository", () => {
    let mockDB: IDatabase;
    let repository: MessageRepository;
    let mockInsert: ReturnType<typeof vi.fn>;

    beforeEach(() => {
        // 1. Create a mock function for the data insertion action
        mockInsert = vi.fn();
        
        // 2. Configure virtual database
        mockDB = {
            insert: mockInsert,
            findOne: vi.fn(),
            update: vi.fn(),
            delete: vi.fn()
        } as unknown as IDatabase;

        // 3. Inject virtual DB into Repository
        repository = new MessageRepository(mockDB, mockDB);
    });

    it("should insert a new message successfully", async () => {
        // ARRANGE: Prepare input data and simulated DB return results
        const testMessage: Message = {
            conversation_id: "conv-1",
            sender_id: "user-1",
            content: "Hello World",
            type: "text",
            status: "sent"
        };

        const returnedMessageFromDB = {
            id: "msg-123",
            ...testMessage,
            created_at: new Date()
        };

        // Simulate that calling db.insert will return the newly inserted object
        mockInsert.mockResolvedValue(returnedMessageFromDB);

        // ACT: Execute create function
        const result = await repository.create(testMessage);

        // ASSERT: Check results
        // Make sure the create function returns the correct data from the DB
        expect(result).toEqual(returnedMessageFromDB);
        
        // Make sure the DB calls the correct 'messages' table and transmits the correct data
        expect(mockInsert).toHaveBeenCalledWith('messages', testMessage);
    });

    it("should throw an error if database insert fails", async () => {
        // ARRANGE
        const testMessage: Message = {
            conversation_id: "conv-1",
            sender_id: "user-1",
            content: "Hello",
            type: "text",
            status: "sent"
        };

        // Program the fake insert function to throw an error
        const dbError = new Error("Database connection failed");
        mockInsert.mockRejectedValue(dbError);

        // ACT & ASSERT: Error when calling create function
        await expect(repository.create(testMessage)).rejects.toThrow("Database connection failed");
    });
});
