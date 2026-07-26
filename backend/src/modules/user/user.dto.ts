import type { User } from "./entities/user.entity.js";
import { z } from "zod";

export type UserResponse = {
    id: string;
    email: string;
    name: string;
    student_id?: string;
    phone?: string;
    is_online?: boolean | false,
    avatar_url?: string;
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
            avatar_url: entity.avatar_url
        };
    }
}
export type UpdateUserProfileDto = Partial<
    Omit<UserResponse, "id" | "createdAt" | "updatedAt">
>;

export const GoogleLoginSchema = z.object({
    body: z.object({
        idToken: z
            .string({ message: "ID Token là bắt buộc và phải là chuỗi" }) // Bắt lỗi nếu không truyền hoặc sai kiểu
            .min(1, "ID Token không được để trống"),                       // Bắt lỗi nếu truyền chuỗi rỗng ""
    }).strict()
});


export const UpdateProfileSchema = z.object({
    body: z.object({
        name: z.string().optional(),
        phone: z.string().optional(),
        avatar_url: z.url("URL không hợp lệ").optional(),
    }).strict(),
});


export const GetByIDParams = z.object({
    params: z.object({
        id: z.string({ message: "ID là bắt buộc" }),
    }).strict()
})

// Trích xuất Type từ Schema nếu cần sử dụng ở các tầng khác
export type GoogleLoginInput = z.infer<typeof GoogleLoginSchema>;
export type GetByIDParams = z.infer<typeof GetByIDParams>;
export type UpdateProfileSchema = z.infer<typeof UpdateProfileSchema>;
