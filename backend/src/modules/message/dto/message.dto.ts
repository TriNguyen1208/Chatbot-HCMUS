import { z } from "zod";

const objectIdSchema = z.string().regex(/^[0-9a-fA-F]{24}$/, "Invalid ID format");

export const SendMessageSchema = z.object({
    body: z.object({
        conversation_id: objectIdSchema.optional(),
        receiver_id: z.string().optional(),
        content: z.string().optional(),
        type: z.enum(['text', 'file', 'link', 'image', 'video', 'ai', 'system']).default('text'),
        tag_ids: z.array(z.string()).optional(),
        image: z.object({
            url: z.string(),
            file_key: z.string().optional()
        }).optional(),
        video: z.object({
            url: z.string().optional(),
            file_key: z.string(),
            thumbnail_url: z.string().optional()
        }).optional(),
        status: z.enum(['sent', 'received', 'recalled', 'removed']).optional()
    }).refine(data => data.conversation_id || data.receiver_id, {
        message: "Must provide either conversation_id or receiver_id"
    })
});

export const EditMessageSchema = z.object({
    params: z.object({
        id: objectIdSchema
    }),
    body: z.object({
        content: z.string().min(1, "Content cannot be empty")
    })
});

export const MessageIdParamSchema = z.object({
    params: z.object({
        id: objectIdSchema
    })
});

export const GetMessageListQuerySchema = z.object({
    params: z.object({
        conversation_id: objectIdSchema
    }),
    query: z.object({
        limit: z.string().optional().transform(val => val ? parseInt(val) : 20),
        cursor_id: objectIdSchema.optional()
    })
});

export type SendMessageDto = z.infer<typeof SendMessageSchema>['body'];
export type EditMessageDto = z.infer<typeof EditMessageSchema>['body'];
export type MessageIdParamDto = z.infer<typeof MessageIdParamSchema>['params'];
export type GetMessageListQueryDto = z.infer<typeof GetMessageListQuerySchema>['query'];
export type GetMessageListParamDto = z.infer<typeof GetMessageListQuerySchema>['params'];

export const ToggleReactionSchema = z.object({
    params: z.object({
        id: objectIdSchema
    }),
    body: z.object({
        emoji: z.string().min(1, "Emoji cannot be empty")
    })
});

export type ToggleReactionDto = z.infer<typeof ToggleReactionSchema>['body'];
