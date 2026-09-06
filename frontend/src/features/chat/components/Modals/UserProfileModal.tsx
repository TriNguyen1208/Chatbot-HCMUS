import React, { useEffect, useState } from 'react';
import Image from 'next/image';
import { useModalStore } from '@/features/chat/stores/modalStore';
import { useUserStore } from '@/features/chat/stores/userStore';
import { Loader2, X, User } from 'lucide-react';
import { DEFAULT_AVATAR } from '@/utils/constants';

export default function UserProfileModal() {
  const { isUserProfileModalOpen, selectedUserIdForProfile, closeUserProfileModal } = useModalStore();
  const { users, requestUser } = useUserStore();
  
  const [showImageModal, setShowImageModal] = useState(false);

  useEffect(() => {
    if (isUserProfileModalOpen && selectedUserIdForProfile && !users[selectedUserIdForProfile]) {
      requestUser(selectedUserIdForProfile);
    }
  }, [isUserProfileModalOpen, selectedUserIdForProfile, users, requestUser]);

  if (!isUserProfileModalOpen || !selectedUserIdForProfile) return null;

  const user = users[selectedUserIdForProfile];

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm px-4">
      <div 
        className="w-full max-w-md bg-surface border border-glass-border rounded-3xl p-6 sm:p-8 shadow-2xl relative overflow-hidden"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Background Decorative */}
        <div className="absolute top-[-20%] left-[-20%] w-[60%] h-[60%] rounded-full bg-blue-500/10 blur-[60px] pointer-events-none" />
        <div className="absolute bottom-[-20%] right-[-20%] w-[60%] h-[60%] rounded-full bg-purple-500/10 blur-[60px] pointer-events-none" />

        <button 
          onClick={closeUserProfileModal}
          className="absolute top-4 right-4 p-2 bg-surface/50 hover:bg-hover rounded-full transition-colors text-txt-secondary hover:text-txt-primary z-10"
        >
          <X size={20} />
        </button>

        {!user ? (
          <div className="flex flex-col items-center justify-center h-48">
            <Loader2 className="animate-spin text-brand-primary mb-4" size={32} />
            <p className="text-txt-secondary text-sm">Loading profile...</p>
          </div>
        ) : (
          <div className="flex flex-col items-center relative z-10">
            {/* Avatar */}
            <div 
              className="relative w-28 h-28 mb-4 cursor-pointer group"
              onClick={() => setShowImageModal(true)}
            >
              <Image
                src={user.avatar_url || DEFAULT_AVATAR}
                alt={user.name || "User Avatar"}
                fill
                className="rounded-full object-cover border-4 border-glass-border shadow-md"
              />
              <div className="absolute inset-0 bg-black/40 rounded-full flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity">
                <span className="text-white text-xs font-medium">View</span>
              </div>
            </div>

            {/* Name */}
            <h2 className="text-2xl font-bold text-txt-primary text-center mb-1">
              {user.name}
            </h2>
            <div className="flex items-center gap-1.5 text-txt-extra text-sm mb-6">
              <span className={`w-2 h-2 rounded-full ${user.is_online ? 'bg-green-500' : 'bg-gray-400'}`} />
              {user.is_online ? 'Online' : 'Offline'}
            </div>

            {/* Info Fields */}
            <div className="w-full flex flex-col gap-3">
              <div className="flex items-center p-3 bg-surface/50 border border-glass-border rounded-xl">
                <div className="w-8 h-8 rounded-full bg-brand-primary/10 flex items-center justify-center text-brand-primary mr-3 shrink-0">
                  <User size={16} />
                </div>
                <div className="flex flex-col min-w-0">
                  <span className="text-[11px] text-txt-extra font-medium uppercase tracking-wider">Student ID (MSSV)</span>
                  <span className="text-sm text-txt-primary truncate">{(user as any).studentID || "N/A"}</span>
                </div>
              </div>

              <div className="flex items-center p-3 bg-surface/50 border border-glass-border rounded-xl">
                <div className="w-8 h-8 rounded-full bg-blue-500/10 flex items-center justify-center text-blue-500 mr-3 shrink-0">
                  <span className="font-bold text-sm">@</span>
                </div>
                <div className="flex flex-col min-w-0">
                  <span className="text-[11px] text-txt-extra font-medium uppercase tracking-wider">Email Address</span>
                  <span className="text-sm text-txt-primary truncate">{user.email || "N/A"}</span>
                </div>
              </div>

              <div className="flex items-center p-3 bg-surface/50 border border-glass-border rounded-xl">
                <div className="w-8 h-8 rounded-full bg-purple-500/10 flex items-center justify-center text-purple-500 mr-3 shrink-0">
                  <span className="font-bold text-sm">#</span>
                </div>
                <div className="flex flex-col min-w-0">
                  <span className="text-[11px] text-txt-extra font-medium uppercase tracking-wider">Phone Number</span>
                  <span className="text-sm text-txt-primary truncate">{(user as any).phone || "N/A"}</span>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>

      {showImageModal && user && (
        <div 
          className="fixed inset-0 z-[60] flex items-center justify-center bg-black/90 backdrop-blur-md cursor-zoom-out"
          onClick={() => setShowImageModal(false)}
        >
          <img 
            src={user.avatar_url || DEFAULT_AVATAR} 
            alt="Zoomed avatar" 
            className="max-w-[90vw] max-h-[90vh] object-contain cursor-default" 
            onClick={(e) => e.stopPropagation()} 
          />
          <button 
            onClick={() => setShowImageModal(false)}
            className="absolute top-4 right-4 text-white hover:text-gray-300 p-2 bg-black/50 rounded-full transition-colors cursor-pointer"
          >
            <X size={24} />
          </button>
        </div>
      )}
    </div>
  );
}
