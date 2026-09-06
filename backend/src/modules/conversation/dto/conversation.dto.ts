import { z } from "zod";
import { isUrl } from "#@/shared/utils/url.js";

// MongoDB object IDs are 24-character hex strings
const objectIdSchema = z.string().regex(/^[0-9a-fA-F]{24}$/, "Invalid ID format");

export const CreateConversationSchema = z.object({
    body: z.object({
        member_ids: z.array(z.string()).min(1, "A conversation must have at least 1 other member"),
        type: z.enum(['group', 'utu'], { message: "Conversation type must be group or utu" }),
        name: z.string().optional(),
        avatar_url: z.string("Invalid image URL").optional(),
        primary_icon: z.string().optional().default('👍')
    }).refine((data) => {
        if (data.type === 'group' && !data.name) {
            return false;
        }
        if (data.type === 'utu' && data.avatar_url) {
            return false;
        }
        if (data.avatar_url && !isUrl(data.avatar_url)) {
            return false;
        }
        return true;
    }, {
        message: "Group conversation must have a name",
        path: ["name"]
    })
});

export const GetConversationParamSchema = z.object({
    params: z.object({
        id: objectIdSchema
    })
});

export const AddMembersSchema = z.object({
    params: z.object({
        id: objectIdSchema
    }),
    body: z.object({
        member_ids: z.array(z.string()).min(1, "Must provide at least 1 member")
    })
});

export const RemoveMemberSchema = z.object({
    params: z.object({
        id: objectIdSchema
    }),
    body: z.object({
        member_ids: z.array(z.string()).min(1, "Must provide at least 1 member to remove")
    })
});

export const AssignAdminSchema = z.object({
    params: z.object({
        id: objectIdSchema
    }),
    body: z.object({
        admin_ids: z.array(z.string()).min(1, "Must provide at least 1 admin to assign")
    })
});

export const GetListQuerySchema = z.object({
    query: z.object({
        limit: z.string().optional().transform(val => val ? parseInt(val) : 20),
        cursor_id: objectIdSchema.optional(),
        type: z.enum(['group', 'utu']).optional(),
        search: z.string().optional()
    })
});

export type CreateConversationDto = z.infer<typeof CreateConversationSchema>['body'];
export type GetConversationParamDto = z.infer<typeof GetConversationParamSchema>['params'];
export type AddMembersDto = z.infer<typeof AddMembersSchema>['body'];
export type RemoveMemberDto = z.infer<typeof RemoveMemberSchema>['body'];
export type AssignAdminDto = z.infer<typeof AssignAdminSchema>['body'];
export type GetListQueryDto = z.infer<typeof GetListQuerySchema>['query'];
