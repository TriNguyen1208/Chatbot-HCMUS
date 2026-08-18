import type { User } from "#@/modules/user/entities/user.entity.js";
import { z } from "zod";

export type UserResponse = {
    id: string;
    email: string;
    name: string;
    student_id?: string;
    phone?: string;
    is_online?: boolean | false,
    avatar_url?: string;
    created_at?: Date;
}
export const UserMapper = {
    toUserResponse: (entity: User): UserResponse => {
        return {
            id: entity.id,
            email: entity.email,
            name: entity.name,
            student_id: entity.student_id,
            phone: entity.phone,
            is_online: entity.is_online,
            avatar_url: entity.avatar_url,
            created_at: entity.created_at
        };
    }
}
export type UpdateUserProfileDto = Partial<
    Omit<UserResponse, "id" | "createdAt" | "updatedAt">
>;


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

// Extract Type from Schema if needed to use at other levels
export type GetByIDParams = z.infer<typeof GetByIDParams>;
export type UpdateProfileSchema = z.infer<typeof UpdateProfileSchema>;

export const GetListQuery = z.object({
    query: z.object({
        limit: z.preprocess((val) => (val ? Number(val) : 20), z.number().min(1).max(50)),
        cursor_id: z.string().optional(),
    }).strict()
});
export type GetListQueryDto = z.infer<typeof GetListQuery>["query"];
