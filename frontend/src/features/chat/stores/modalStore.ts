import { create } from 'zustand';
import { Message } from '../types';

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
  isForwardModalOpen: boolean;
  forwardMessageData: Message | null;
  openForwardModal: (message: Message) => void;
  closeForwardModal: () => void;
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
  isForwardModalOpen: false,
  forwardMessageData: null,
  openForwardModal: (message) => set({ isForwardModalOpen: true, forwardMessageData: message }),
  closeForwardModal: () => set({ isForwardModalOpen: false, forwardMessageData: null }),
}));
