import { apiResponse } from "#@/shared/utils/api-response.js";
import type { Request, Response, NextFunction } from "express";
import type { MessageService } from "../services/message.service.js";
import type { SendMessageDto, EditMessageDto, MessageIdParamDto, GetMessageListParamDto, GetMessageListQueryDto, ToggleReactionDto } from "../dto/message.dto.js";

export class MessageController {
    constructor(private readonly messageService: MessageService) { }

    /**
     * Handles sending a new message (text, image, video, etc.).
     * @param req The Express request object containing the user and message payload.
     * @param res The Express response object.
     * @param next The Express next middleware function.
     */
    sendMessage = async (req: Request, res: Response, next: NextFunction) => {
        const userID = req.user!.userID;
        const payload = req.body as SendMessageDto;
        const result = await this.messageService.handleIncomingMessage(userID, payload);
        if(result.status == "queued"){
            return apiResponse.success(res, null, {
                message: result.message
            })
        }
        return apiResponse.success(res, result.data);
    }

    /**
     * Retrieves a paginated list of messages for a specific conversation.
     * @param req The Express request object containing conversation_id and pagination queries.
     * @param res The Express response object.
     * @param next The Express next middleware function.
     */
    getMessages = async (req: Request, res: Response, next: NextFunction) => {
        const userID = req.user!.userID;
        const params = req.params as GetMessageListParamDto;
        const query = req.query as unknown as GetMessageListQueryDto;

        const messages = await this.messageService.getMessages(params.conversation_id, userID, query.limit, query.cursor_id);

        return apiResponse.success(res, messages);
    }

    /**
     * Edits the content of an existing text message.
     * @param req The Express request object containing message ID and new content.
     * @param res The Express response object.
     * @param next The Express next middleware function.
     */
    editMessage = async (req: Request, res: Response, next: NextFunction) => {
        const userID = req.user!.userID;
        const params = req.params as MessageIdParamDto;
        const body = req.body as EditMessageDto;

        await this.messageService.editMessage(params.id, userID, body.content);

        return apiResponse.success(res, null, { message: "Message edited successfully" });
    }

    /**
     * Recalls (un-sends) an existing message.
     * @param req The Express request object containing the message ID.
     * @param res The Express response object.
     * @param next The Express next middleware function.
     */
    recallMessage = async (req: Request, res: Response, next: NextFunction) => {
        const userID = req.user!.userID;
        const params = req.params as MessageIdParamDto;

        await this.messageService.recallMessage(params.id, userID);

        return apiResponse.success(res, null, { message: "Message recalled successfully" });
    }

    /**
     * Toggles a reaction on a message.
     */
    toggleReaction = async (req: Request, res: Response, next: NextFunction) => {
        const userID = req.user!.userID;
        const params = req.params as MessageIdParamDto;
        const body = req.body as ToggleReactionDto;

        const updatedMessage = await this.messageService.toggleReaction(params.id, userID, body.emoji);

        return apiResponse.success(res, updatedMessage, { message: "Reaction toggled successfully" });
    }
}
