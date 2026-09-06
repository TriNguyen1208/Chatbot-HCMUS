import { api } from "@/lib/api";
import type { ApiResponse } from "@/types/type";

export type UpdateProfileDto = {
    phone?: string;
    avatar_url?: string;
};

export type UserProfileResponse = {
    id: string;
    name: string;
    email: string;
    avatar_url: string | null;
    studentID?: string;
    phone?: string;
};

export const profileApi = {
    updateProfile: async (data: UpdateProfileDto): Promise<UserProfileResponse> => {
        const res = await api.patch<ApiResponse<UserProfileResponse>>('/user', data);
        return res.data.data;
    }
};
