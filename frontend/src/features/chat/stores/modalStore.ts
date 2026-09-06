import { create } from 'zustand';

interface ModalState {
  isCreateGroupOpen: boolean;
  setCreateGroupOpen: (isOpen: boolean) => void;
  isKickModalOpen: boolean;
  setKickModalOpen: (isOpen: boolean) => void;
  isAssignAdminModalOpen: boolean;
  setAssignAdminModalOpen: (isOpen: boolean) => void;
  isUserProfileModalOpen: boolean;
  selectedUserIdForProfile: string | null;
  openUserProfileModal: (userId: string) => void;
  closeUserProfileModal: () => void;
}

export const useModalStore = create<ModalState>()((set) => ({
  isCreateGroupOpen: false,
  setCreateGroupOpen: (isOpen) => set({ isCreateGroupOpen: isOpen }),
  isKickModalOpen: false,
  setKickModalOpen: (isOpen) => set({ isKickModalOpen: isOpen }),
  isAssignAdminModalOpen: false,
  setAssignAdminModalOpen: (isOpen) => set({ isAssignAdminModalOpen: isOpen }),
  isUserProfileModalOpen: false,
  selectedUserIdForProfile: null,
  openUserProfileModal: (userId: string) => set({ isUserProfileModalOpen: true, selectedUserIdForProfile: userId }),
  closeUserProfileModal: () => set({ isUserProfileModalOpen: false, selectedUserIdForProfile: null }),
}));
