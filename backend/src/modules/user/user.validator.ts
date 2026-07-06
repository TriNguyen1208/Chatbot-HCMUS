import { z } from "zod";


export const GoogleLoginSchema = z.object({
    body: z.object({
        idToken: z
            .string({ message: "ID Token là bắt buộc và phải là chuỗi" }) // Bắt lỗi nếu không truyền hoặc sai kiểu
            .min(1, "ID Token không được để trống"),                       // Bắt lỗi nếu truyền chuỗi rỗng ""
    })
});

// Validation cho Refresh Token
export const RefreshTokenSchema = z.object({
    body: z.object({
        refreshToken: z
            .string({ message: "Refresh token là bắt buộc" }) // Bắt lỗi nếu không truyền hoặc sai kiểu
            .min(1, "Refresh token không được để trống"),   
    })                   
});

// Validation cho Logout
export const LogoutSchema = z.object({
    body: z.object({
        refreshToken: z
            .string({ message: "Refresh token là bắt buộc" }) // Bắt lỗi nếu không truyền hoặc sai kiểu
            .min(1, "Refresh token không được để trống"),                       // Bắt lỗi nếu truyền chuỗi rỗng ""
    })
});

// Trích xuất Type từ Schema nếu cần sử dụng ở các tầng khác
export type GoogleLoginInput = z.infer<typeof GoogleLoginSchema>;
export type RefreshTokenInput = z.infer<typeof RefreshTokenSchema>;