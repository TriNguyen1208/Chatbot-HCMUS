import { useState, useRef, useEffect } from "react";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { profileApi } from "../api/profileApi";
import { mediaApi } from "@/features/chat/api/media.api";
import toast from "react-hot-toast";
import { useRouter } from "next/navigation";

export const useProfileForm = () => {
    const { user: currentUser, setUser } = useAuthStore();
    const router = useRouter();

    const [phone, setPhone] = useState(currentUser?.phone || "");
    const [avatarUrl, setAvatarUrl] = useState(currentUser?.avatar_url || "");
    const [isUploading, setIsUploading] = useState(false);
    const [isSaving, setIsSaving] = useState(false);

    const fileInputRef = useRef<HTMLInputElement>(null);

    // Sync state when currentUser loads (if it takes time)
    useEffect(() => {
        if (currentUser) {
            if (currentUser.phone) setPhone(currentUser.phone);
            if (currentUser.avatar_url) setAvatarUrl(currentUser.avatar_url);
        }
    }, [currentUser]);

    const handleAvatarClick = () => {
        if (fileInputRef.current) {
            fileInputRef.current.click();
        }
    };

    const handleFileChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
        const file = e.target.files?.[0];
        if (!file) return;

        // Reset input value to allow selecting the same file again
        e.target.value = '';

        if (!file.type.startsWith("image/")) {
            // toast.error("Please select an image file.");
            return;
        }

        try {
            setIsUploading(true);
            const res = await mediaApi.uploadImage(file);
            const url = res.data.resource_url;
            if (url) {
                setAvatarUrl(url);
                // toast.success("Avatar uploaded! Remember to save changes.");
            }
        } catch (error) {
            console.error("Failed to upload avatar", error);
            // toast.error("Failed to upload avatar.");
        } finally {
            setIsUploading(false);
        }
    };

    const handleSave = async (e: React.FormEvent) => {
        e.preventDefault();

        if (!currentUser) return;

        try {
            setIsSaving(true);
            const dataToUpdate = {
                phone: phone.trim() !== currentUser.phone ? phone.trim() : undefined,
                avatar_url: avatarUrl !== currentUser.avatar_url ? avatarUrl : undefined,
            };

            // Only update if there are changes
            if (Object.keys(dataToUpdate).some(key => dataToUpdate[key as keyof typeof dataToUpdate] !== undefined)) {
                const updatedUser = await profileApi.updateProfile(dataToUpdate);
                // toast.success("Profile updated successfully!");
                setUser({ ...currentUser, ...dataToUpdate });
                router.push("/chat");
            } else {
                // toast("No changes to save.", { icon: "ℹ️" });
                router.push("/chat");
            }
        } catch (error) {
            console.error("Failed to update profile", error);
            // toast.error("Failed to update profile.");
        } finally {
            setIsSaving(false);
        }
    };

    const handleCancel = () => {
        router.push("/chat");
    };

    return {
        currentUser,
        phone,
        setPhone,
        avatarUrl,
        isUploading,
        isSaving,
        fileInputRef,
        handleAvatarClick,
        handleFileChange,
        handleSave,
        handleCancel
    };
};
