// Import necessary functions from test library "vitest"
// describe: Group test cases into a cluster.
// it: Defines a specific test case (a test scenario).
// expect: Used to compare actual results with expected results.
// vi: Object containing vitest's mocking tools.
// beforeEach: The function will be run automatically before each "it" (test case) starts.
import { describe, it, expect, vi, beforeEach } from "vitest";

// Import file to test (ConversationRepository)
import { ConversationRepository } from "../repositories/conversation.repository.js";

// Import the IDatabase interface as a data type for the virtual database
import type { IDatabase } from "#@/infrastructure/database/database.interface.js";

// Start a test group specifically for the "ConversationRepository" class
describe("ConversationRepository", () => {
    // Declare the mockDB variable to store a virtual (fake) version of the Database
    let mockDB: IDatabase;
    
    // Declare a repository variable to contain the actual class we will test
    let repository: ConversationRepository;
    
    // Declare the variable mockFindOne to store a function that simulates searching in the DB
    let mockFindOne: ReturnType<typeof vi.fn>;

    // This function will automatically run BEFORE EACH test case (it) below
    // The purpose is to reset the initial state, avoiding previous test case data affecting the following test case.
    beforeEach(() => {
        // Create a completely empty mock function using vi.fn() command
        mockFindOne = vi.fn();
        
        // Create a virtual Database (mockDB) that simulates IDatabase
        mockDB = {
            // Assign the newly created pseudo function to the findOne property
            findOne: mockFindOne,
            // Simulate other functions of IDatabase (not used in this test so leave blank)
            insert: vi.fn(),
            update: vi.fn(),
            delete: vi.fn()
        } as unknown as IDatabase; // Cast so that TypeScript doesn't report missing other functions

        // Inject virtual database into Repository. 
        // Instead of Repository calling down to the real Supabase, it will call into mockDB.
        repository = new ConversationRepository(mockDB, mockDB);
    });

    // TEST CASE 1: Check if the user is counted as a member if the ID is in the member_ids array
    it("should return true if user is in member_ids array", async () => {
        // 1. PREPARE VIRTUAL DATA (Arrange)
        // Programming a fake search function: "When you are called, return the result as an object whose member array contains user-1"
        mockFindOne.mockResolvedValue({
            member_ids: ["user-1", "user-2"]
        });

        // 2. EXECUTE (Act)
        // Call the actual function to be tested in the Repository
        const isMember = await repository.checkUserInConversation("conv-id", "user-1");

        // 3. CHECK (Assert)
        // Expect the isMember variable to have the value true
        expect(isMember).toBe(true);
        // Check if the Repository sends the correct search command to the DB (does it transmit the correct table name and ID)
        expect(mockFindOne).toHaveBeenCalledWith('conversations', { id: 'conv-id' });
    });

    // TEST CASE 2: Check if the user is NOT present in the member_ids array
    it("should return false if user is NOT in member_ids array", async () => {
        // 1. PREPARE VIRTUAL DATA
        // Simulate a DB that returns a conversation with only user-1 and user-2
        mockFindOne.mockResolvedValue({
            member_ids: ["user-1", "user-2"]
        });

        // 2. EXECUTION
        // Ask if "user-3" is in this conversation
        const isMember = await repository.checkUserInConversation("conv-id", "user-3");

        // 3. CHECK
        // Expect the result to be false (because user-3 is not in the returned array)
        expect(isMember).toBe(false);
    });

    // TEST CASE 3: Test the case where the database cannot find the chat group (the conversation does not exist)
    it("should return false if conversation is not found", async () => {
        // 1. PREPARE VIRTUAL DATA
        // Simulate DB not found (returns null)
        mockFindOne.mockResolvedValue(null);

        // 2. EXECUTION
        // Check user-1 in the "conv-id" conversation (this conversation is assumed to not exist)
        const isMember = await repository.checkUserInConversation("conv-id", "user-1");
        
        // 3. CHECK
        // Expect the Repository's code logic to process safely (return false) instead of crashing the app.
        expect(isMember).toBe(false);
    });
});
