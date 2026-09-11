"use client";
import React from "react";
import Image from "next/image";
import { ArrowLeft, X } from "lucide-react";
import { DEFAULT_AVATAR } from "@/config/constants";
import { useChatStore } from "@/features/chat/stores/chatStore";
import { useConversationInfo } from "./useConversationInfo";
import { InfoHeader } from "./components/InfoHeader";
import { MemberList } from "./components/MemberList";
import { MediaGallery } from "./components/MediaGallery";
import { DangerActions, QuickActions } from "./components/DangerActions";
import { EditGroupModal } from "./components/EditGroupModal";
import { BlockUserModal } from "./components/BlockUserModal";
import { DisbandGroupModal } from "./components/DisbandGroupModal";
import { MediaViewerModal } from "./components/MediaViewerModal";

const ConversationInfo = () => {
  const info = useConversationInfo();

  if (!info.showInfoPanel || !info.activeConversation) return null;

  return (
    <div 
      className="relative flex flex-col h-full bg-surface/50 backdrop-blur-xl shrink-0 transition-none border-l border-glass-border overflow-hidden"
      style={{ width: `${info.panelWidth}px` }}
    >
      {/* Resizer Handle */}
      <div 
        className="absolute left-0 top-0 bottom-0 w-1.5 cursor-col-resize hover:bg-brand-primary/50 transition-colors z-10"
        onMouseDown={info.handleMouseDown}
      />

      <div className="flex flex-col w-full h-full overflow-y-auto custom-scrollbar">
        {info.isSearchMode ? (
          <>
            {/* Search Header */}
            <div className="h-[64px] flex items-center gap-3 px-4 border-b border-glass-border shrink-0">
              <button 
                onClick={() => info.setIsSearchMode(false)}
                className="p-2 rounded-full hover:bg-glass cursor-pointer text-gray-500 hover:text-text-primary transition-colors shrink-0"
              >
                <ArrowLeft size={20} />
              </button>
              <div className="flex-1 bg-surface-solid rounded-xl border border-glass-border flex items-center px-3 py-1.5 h-[36px]">
                <input 
                  autoFocus
                  placeholder="Tìm kiếm tin nhắn..."
                  className="bg-transparent border-none outline-none text-sm w-full text-text-primary placeholder:text-text-secondary"
                  value={info.searchQuery}
                  onChange={(e) => info.setSearchQuery(e.target.value)}
                />
                {info.searchQuery && (
                  <button onClick={() => info.setSearchQuery("")} className="text-gray-400 hover:text-text-primary ml-2 shrink-0">
                    <X size={16} />
                  </button>
                )}
              </div>
            </div>
            
            {/* Search Results */}
            <div className="flex-1 overflow-y-auto p-2 space-y-1">
              {info.isSearching && <div className="text-center text-sm text-text-secondary py-4">Đang tìm kiếm...</div>}
              {!info.isSearching && info.searchQuery && info.searchResults.length === 0 && (
                <div className="text-center text-sm text-text-secondary py-4">Không tìm thấy kết quả.</div>
              )}
              {!info.isSearching && info.searchResults.map((result) => (
                <div 
                  key={result.id}
                  onClick={() => {
                    info.setTargetMessageId(result.id);
                  }}
                  className="flex items-start gap-3 p-3 rounded-xl hover:bg-glass-panel cursor-pointer transition-colors group"
                >
                  <div className="relative shrink-0">
                    <Image
                      src={result.sender?.avatar_url || DEFAULT_AVATAR}
                      alt="avatar"
                      width={40}
                      height={40}
                      className="rounded-full object-cover size-10 shadow-sm border border-glass-border"
                    />
                  </div>
                  <div className="flex flex-col min-w-0 flex-1">
                    <div className="flex justify-between items-baseline mb-0.5">
                      <span className="text-sm font-semibold text-text-primary truncate">
                        {result.sender?.name || "Người dùng"}
                      </span>
                    </div>
                    <p className="text-sm text-text-secondary line-clamp-2">
                      {result.content}
                    </p>
                  </div>
                </div>
              ))}
            </div>
          </>
        ) : info.mediaMode ? (
          <MediaGallery
            mediaMode={info.mediaMode}
            setMediaMode={info.setMediaMode}
            mediaItems={info.mediaItems}
            isLoadingMedia={info.isLoadingMedia}
            isMediaExpanded={info.isMediaExpanded}
            setIsMediaExpanded={info.setIsMediaExpanded}
            setSelectedMedia={info.setSelectedMedia}
          />
        ) : (
          <>
            <InfoHeader
              toggleInfoPanel={info.toggleInfoPanel}
              displayAvatar={info.displayAvatar}
              displayName={info.displayName}
              activeConversation={info.activeConversation}
              otherMember={info.otherMember}
              isOnline={info.isOnline}
            />

            <QuickActions
              activeConversation={info.activeConversation}
              isAdmin={Boolean(info.isAdmin)}
              setIsSearchMode={info.setIsSearchMode}
              setCreateGroupOpen={info.setCreateGroupOpen}
              setShowEditGroupModal={info.setShowEditGroupModal}
            />

            <MediaGallery
              mediaMode={info.mediaMode}
              setMediaMode={info.setMediaMode}
              mediaItems={info.mediaItems}
              isLoadingMedia={info.isLoadingMedia}
              isMediaExpanded={info.isMediaExpanded}
              setIsMediaExpanded={info.setIsMediaExpanded}
              setSelectedMedia={info.setSelectedMedia}
            />

            <MemberList
              activeConversation={info.activeConversation}
              isMembersExpanded={info.isMembersExpanded}
              setIsMembersExpanded={info.setIsMembersExpanded}
              users={info.users}
              currentUserId={info.user?.id}
              openUserProfileModal={info.openUserProfileModal}
            />

            <DangerActions
              activeConversation={info.activeConversation}
              isAdmin={Boolean(info.isAdmin)}
              setIsSearchMode={info.setIsSearchMode}
              setCreateGroupOpen={info.setCreateGroupOpen}
              setShowEditGroupModal={info.setShowEditGroupModal}
              setAssignAdminModalOpen={info.setAssignAdminModalOpen}
              setKickModalOpen={info.setKickModalOpen}
              handleLeaveGroup={info.handleLeaveGroup}
              setShowDisbandModal={info.setShowDisbandModal}
              isDisbanding={info.isDisbanding}
              currentUserId={info.user?.id}
              handleUnblockUser={info.handleUnblockUser}
              setShowBlockModal={info.setShowBlockModal}
              isBlocking={info.isBlocking}
            />
          </>
        )}
      </div>

      <MediaViewerModal 
        isOpen={!!info.selectedMedia}
        onClose={() => info.setSelectedMedia(null)}
        mediaType={info.selectedMedia?.type || 'image'}
        mediaUrl={info.selectedMedia?.url || ''}
      />

      <EditGroupModal 
        isOpen={info.showEditGroupModal}
        onClose={() => info.setShowEditGroupModal(false)}
        conversation={info.activeConversation}
        onSuccess={(updatedConv) => {
          const { setActiveConversation } = useChatStore.getState();
          setActiveConversation(updatedConv);

          const updateQueryCache = (queryKey: any[]) => {
            info.queryClient.setQueryData(queryKey, (oldData: any) => {
              if (!oldData || !oldData.pages) return oldData;
              return {
                ...oldData,
                pages: oldData.pages.map((page: any[]) =>
                  page.map((conv: any) =>
                    conv.id === updatedConv.id ? { ...conv, ...updatedConv } : conv
                  )
                ),
              };
            });
          };

          updateQueryCache(['conversations']);
          updateQueryCache(['conversations', 'group']);
        }}
      />

      <BlockUserModal
        isOpen={info.showBlockModal}
        onClose={() => info.setShowBlockModal(false)}
        onConfirm={info.handleBlockUser}
        userName={info.displayName}
        userAvatar={info.displayAvatar}
        userEmail={info.otherMember?.email}
        isSubmitting={info.isBlocking}
      />

      <DisbandGroupModal
        isOpen={info.showDisbandModal}
        onClose={() => info.setShowDisbandModal(false)}
        onConfirm={info.handleDisbandGroup}
        groupName={info.displayName}
        groupAvatar={info.displayAvatar}
        memberCount={info.activeConversation?.member_ids?.length || 0}
        isSubmitting={info.isDisbanding}
      />
    </div>
  );
};

export default ConversationInfo;
