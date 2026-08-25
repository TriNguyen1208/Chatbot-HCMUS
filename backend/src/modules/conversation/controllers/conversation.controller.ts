import type { Request, Response } from "express";
import { ConversationService } from "../services/conversation.service.js";
import type { CreateConversationDto, GetConversationParamDto, AddMembersDto, RemoveMemberDto, AssignAdminDto, GetListQueryDto } from "../dto/conversation.dto.js";
import { apiResponse } from "#@/shared/utils/api-response.js";

export class ConversationController {
    constructor(private readonly conversationService: ConversationService) {}

    /**
     * Handles the creation of a new conversation (either 1-1 or group).
     * @param req The Express request object containing the user and body data.
     * @param res The Express response object.
     */
    createConversation = async (req: Request, res: Response) => {
        const userId = req.user!.userID;
        const data = req.body as CreateConversationDto;

        const conversation = await this.conversationService.createConversation(userId, data);
        console.log("Controller: ", conversation)
        return apiResponse.success(res, conversation, {
            statusCode: 201,
            message: "Conversation created successfully"
        })
    }

    /**
     * Retrieves a specific conversation by its ID.
     * @param req The Express request object containing the conversation ID in params.
     * @param res The Express response object.
     */
    getConversation = async (req: Request, res: Response) => {
        const userId = req.user!.userID;
        const params = req.params as GetConversationParamDto;

        const conversation = await this.conversationService.getConversationById(params.id, userId);

        return apiResponse.success(res, conversation, {
            statusCode: 200,
            message: "Retrieving conversation information successfully"
        })
    }


    /**
     * Retrieves a paginated list (based on last message) of conversations for the current user.
     * @param req The Express request object containing pagination queries.
     * @param res The Express response object.
     */
    getList = async (req: Request, res: Response) => {
        const userId = req.user!.userID;
        const {limit, cursor_id, type} = req.query as unknown as GetListQueryDto;
        
        const list = await this.conversationService.getConversationList(userId, limit, cursor_id, type);
        
        return apiResponse.success(res, list, {
            statusCode: 200,
            message: "Conversations retrieved successfully"
        });
    }

    /**
     * Adds new members to an existing group conversation.
     * Requires admin privileges.
     * @param req The Express request object.
     * @param res The Express response object.
     */
    addMember = async (req: Request, res: Response) => {
        const adminId = req.user!.userID;
        const params = req.params as GetConversationParamDto;
        const body = req.body as AddMembersDto;

        await this.conversationService.addMember(adminId, params.id, body.member_ids);

        return apiResponse.success(res, null, {
            statusCode: 200,
            message: "Members added successfully"
        });
    }

    /**
     * Removes multiple members from an existing group conversation.
     * Requires admin privileges.
     * @param req The Express request object.
     * @param res The Express response object.
     */
    removeMember = async (req: Request, res: Response) => {
        const adminId = req.user!.userID;
        const params = req.params as GetConversationParamDto;
        const body = req.body as RemoveMemberDto;

        await this.conversationService.removeMembers(adminId, params.id, body.member_ids);

        return apiResponse.success(res, null, {
            statusCode: 200,
            message: "Members removed successfully"
        });
    }

    /**
     * Assigns admin status to members in a group conversation.
     * Requires admin privileges.
     * @param req The Express request object.
     * @param res The Express response object.
     */
    assignAdmins = async (req: Request, res: Response) => {
        const adminId = req.user!.userID;
        const params = req.params as GetConversationParamDto;
        const body = req.body as AssignAdminDto;

        await this.conversationService.assignAdmins(adminId, params.id, body.admin_ids);

        return apiResponse.success(res, null, {
            statusCode: 200,
            message: "Admins assigned successfully"
        });
    }

    /**
     * Allows the current user to leave a group conversation.
     * Admins cannot leave the group directly (must transfer ownership first).
     * @param req The Express request object.
     * @param res The Express response object.
     */
    leaveGroup = async (req: Request, res: Response) => {
        const userId = req.user!.userID;
        const params = req.params as GetConversationParamDto;

        await this.conversationService.leaveGroup(userId, params.id);

        return apiResponse.success(res, null, {
            statusCode: 200,
            message: "Left group successfully"
        });
    }
}
