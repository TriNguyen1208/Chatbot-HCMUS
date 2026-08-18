import { useAuthStore } from "@/features/auth/stores/authStore";
import { authApi } from "@/features/auth/api/authApi";

export const useSidebarProfile = () => {
  const { user, clearUser } = useAuthStore();

  const handleLogout = async () => {
    try {
      await authApi.logout();
    } catch (e) {
      console.error(e);
    } finally {
      clearUser();
      window.location.href = "/";
    }
  };

  return {
    user,
    handleLogout
  };
};
