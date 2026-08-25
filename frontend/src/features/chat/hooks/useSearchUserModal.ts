import { useState, useEffect } from "react";
import { userApi } from "@/features/chat/api/user.api";
import { User } from "@/features/chat/types";
import { useRouter } from "next/navigation";

export const useSearchUserModal = (isOpen: boolean, onClose: () => void) => {
  const [users, setUsers] = useState<User[]>([]);
  const router = useRouter();

  useEffect(() => {
    if (isOpen) {
      userApi.getUsers()
        .then(res => setUsers(res.data || res))
        .catch(console.error);
    }
  }, [isOpen]);

  const handleUserClick = (userId: string) => {
    router.push(`?receiver_id=${userId}`);
    onClose();
  };

  return {
    users,
    handleUserClick
  };
};
