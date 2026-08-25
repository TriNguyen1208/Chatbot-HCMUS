import type { User } from "#@/modules/user/entities/user.entity.js";
import { z } from "zod";


export const UpdateProfileSchema = z.object({
    body: z.object({
        name: z.string().optional(),
        phone: z.string().optional(),
        avatar_url: z.url("Invalid URL").optional(),
    }).strict(),
});


export const GetByIDParams = z.object({
    params: z.object({
        id: z.string({ message: "ID is required" }),
    }).strict()
})

export const GetListQuery = z.object({
    query: z.object({
        limit: z.preprocess((val) => (val ? Number(val) : 20), z.number().min(1).max(50)),
        cursor_id: z.string().optional(),
    }).strict()
});
// Extract Type from Schema if needed to use at other levels
export type GetByIDParamsDto = z.infer<typeof GetByIDParams>['params'];
export type UpdateProfileDto = z.infer<typeof UpdateProfileSchema>['body'];
export type GetListQueryDto = z.infer<typeof GetListQuery>["query"];
