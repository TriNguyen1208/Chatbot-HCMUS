"use client";
import { useState, useEffect } from "react";
import { userApi } from "@/features/chat/api/user.api";
import { User } from "@/types";
import { useRouter, usePathname } from "next/navigation";

export const useSearchUserModal = (isOpen: boolean, onClose: () => void) => {
  const [users, setUsers] = useState<User[]>([]);
  const router = useRouter();
  const pathname = usePathname();
  const basePath = pathname.startsWith('/direct-chat') || pathname.startsWith('/chat') ? pathname : '/direct-chat';

  useEffect(() => {
    if (isOpen) {
      userApi.getUsers()
        .then(res => setUsers(res))
        .catch(console.error);
    }
  }, [isOpen]);

  const handleUserClick = (userId: string) => {
    router.push(`${basePath}?receiver_id=${userId}`);
    onClose();
  };

  return {
    users,
    handleUserClick
  };
};
