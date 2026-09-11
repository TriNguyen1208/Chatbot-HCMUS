"use client";
import { useState, useEffect } from 'react';
import { useModalStore } from '@/features/chat/stores/modalStore';
import { useUserStore } from '@/features/chat/stores/userStore';

export const useUserProfileModal = () => {
  const { isUserProfileModalOpen, selectedUserIdForProfile, closeUserProfileModal } = useModalStore();
  const { users, requestUser } = useUserStore();
  const [showImageModal, setShowImageModal] = useState(false);

  useEffect(() => {
    if (isUserProfileModalOpen && selectedUserIdForProfile && !users[selectedUserIdForProfile]) {
      requestUser(selectedUserIdForProfile);
    }
  }, [isUserProfileModalOpen, selectedUserIdForProfile, users, requestUser]);

  const user = selectedUserIdForProfile ? users[selectedUserIdForProfile] : null;

  return {
    isOpen: isUserProfileModalOpen,
    user,
    showImageModal,
    setShowImageModal,
    closeUserProfileModal,
  };
};
