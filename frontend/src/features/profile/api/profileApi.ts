import { http } from "@/lib/api";
import type { User, UpdateProfileDto } from "@/types";

export type { UpdateProfileDto };
export type UserProfileResponse = User;

export const profileApi = {
    updateProfile: async (data: UpdateProfileDto): Promise<User> => {
        return http.patch<User>('/user', data);
    }
};
