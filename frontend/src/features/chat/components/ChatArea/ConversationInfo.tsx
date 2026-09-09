"use client";
import React, { useState, useRef, useCallback, useEffect } from "react";
import { useQueryClient } from "@tanstack/react-query";
import Image from "next/image";
import { useRouter } from "next/navigation";
import { X, Bell, Search, UserPlus, LogOut, ChevronRight, ChevronDown, ShieldCheck, UserMinus, Settings, ArrowLeft, Image as ImageIcon, Video as VideoIcon, File as FileIcon, Ban, Trash2 } from "lucide-react";
import { useChatStore } from "@/features/chat/stores/chatStore";
import { useSearchStore } from "@/features/chat/stores/searchStore";
import { useChatHeader } from "@/features/chat/hooks/useChatHeader";
import { useUserStore } from "@/features/chat/stores/userStore";
import { getRelativeTime } from "@/utils/formatTime";
import { DEFAULT_AVATAR } from "@/utils/constants";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { useModalStore } from "@/features/chat/stores/modalStore";
import { messageApi } from "@/features/chat/api/message.api";
import { conversationApi } from "@/features/chat/api/conversation.api";
import { SearchResult } from "@/features/chat/api/search.api";
import MediaViewerModal from "../Modals/MediaViewerModal";
import { EditGroupModal } from "../Modals/EditGroupModal";
import BlockUserModal from "../Modals/BlockUserModal";
import DisbandGroupModal from "../Modals/DisbandGroupModal";

const MIN_WIDTH = 250;
const MAX_WIDTH = 500;
const DEFAULT_WIDTH = 320;

const ConversationInfo = () => {
  const router = useRouter();
  const queryClient = useQueryClient();
  const showInfoPanel = useChatStore((state) => state.showInfoPanel);
  const toggleInfoPanel = useChatStore((state) => state.toggleInfoPanel);
  const setTargetMessageId = useSearchStore((state) => state.setTargetMessageId);
  const { user } = useAuthStore();
  const users = useUserStore((state) => state.users);

  const {
    setCreateGroupOpen,
    setAssignAdminModalOpen,
    setKickModalOpen,
    openUserProfileModal
  } = useModalStore();

  const {
    activeConversation,
    displayName,
    displayAvatar,
    otherMember,
    isOnline,
    handleLeaveGroup,
    isAdmin,
  } = useChatHeader();
  console.log(activeConversation)
  const [panelWidth, setPanelWidth] = useState(DEFAULT_WIDTH);
  const isResizing = useRef(false);
  
  const [isMembersExpanded, setIsMembersExpanded] = useState(true);
  const [isMediaExpanded, setIsMediaExpanded] = useState(true);

  // Media Mode State
  const [mediaMode, setMediaMode] = useState<'image' | 'video' | null>(null);
  const [mediaItems, setMediaItems] = useState<any[]>([]);
  const [isLoadingMedia, setIsLoadingMedia] = useState(false);
  const [selectedMedia, setSelectedMedia] = useState<{ url: string, type: 'image' | 'video' } | null>(null);

  const [showEditGroupModal, setShowEditGroupModal] = useState(false);
  const [showBlockModal, setShowBlockModal] = useState(false);
  const [showDisbandModal, setShowDisbandModal] = useState(false);
  const [isBlocking, setIsBlocking] = useState(false);
  const [isDisbanding, setIsDisbanding] = useState(false);

  const handleDisbandGroup = async () => {
    if (!activeConversation?.id || isDisbanding) return;

    try {
      setIsDisbanding(true);
      const convId = activeConversation.id as string;
      await conversationApi.disbandGroup(convId);

      // Invalidate queries
      queryClient.invalidateQueries({ queryKey: ["conversations"] });
      queryClient.invalidateQueries({ queryKey: ["conversations", "group"] });

      // Close modal and redirect
      setShowDisbandModal(false);
      useChatStore.getState().setActiveConversation(null);
      router.push("/group-chat");
    } catch (error: any) {
      console.error("Lỗi khi giải tán nhóm:", error);
      alert(error?.response?.data?.message || error?.message || "Không thể giải tán nhóm");
    } finally {
      setIsDisbanding(false);
    }
  };

  const handleBlockUser = async () => {
    if (!activeConversation?.id || isBlocking) return;

    try {
      setIsBlocking(true);
      const res = await conversationApi.blockConversation(activeConversation.id as string);
      const updatedConv = (res as any).data || res;
      useChatStore.getState().setActiveConversation(updatedConv);
      queryClient.invalidateQueries({ queryKey: ["conversations"] });
      setShowBlockModal(false);
    } catch (error: any) {
      console.error("Lỗi khi chặn người dùng:", error);
      alert(error?.response?.data?.message || error?.message || "Không thể chặn người dùng");
    } finally {
      setIsBlocking(false);
    }
  };

  const handleUnblockUser = async () => {
    if (!activeConversation?.id || isBlocking) return;
    try {
      setIsBlocking(true);
      const res = await conversationApi.unblockConversation(activeConversation.id as string);
      const updatedConv = (res as any).data || res;
      useChatStore.getState().setActiveConversation(updatedConv);
      queryClient.invalidateQueries({ queryKey: ["conversations"] });
    } catch (error: any) {
      console.error("Lỗi khi bỏ chặn người dùng:", error);
      alert(error?.response?.data?.message || error?.message || "Không thể bỏ chặn người dùng");
    } finally {
      setIsBlocking(false);
    }
  };

  useEffect(() => {
    if (!mediaMode || !activeConversation) {
      setMediaItems([]);
      return;
    }

    const fetchMedia = async () => {
      setIsLoadingMedia(true);
      try {
        const res = await messageApi.getMessages(activeConversation.id as string, 50, undefined, undefined, mediaMode);
        const resultsArray = Array.isArray((res as any).data) ? (res as any).data : res;
        setMediaItems(resultsArray);
      } catch (error) {
        console.error("Fetch media failed:", error);
      } finally {
        setIsLoadingMedia(false);
      }
    };
    fetchMedia();
  }, [mediaMode, activeConversation?.id]);

  // Search Mode State
  const [isSearchMode, setIsSearchMode] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [searchResults, setSearchResults] = useState<SearchResult[]>([]);
  const [isSearching, setIsSearching] = useState(false);
  const debounceTimeout = useRef<NodeJS.Timeout | null>(null);

  useEffect(() => {
    if (!isSearchMode || !activeConversation) {
      setSearchQuery("");
      setSearchResults([]);
      return;
    }
    if (!searchQuery.trim()) {
      setSearchResults([]);
      return;
    }

    setIsSearching(true);
    if (debounceTimeout.current) clearTimeout(debounceTimeout.current);

    debounceTimeout.current = setTimeout(async () => {
      try {
        const res = await messageApi.getMessages(activeConversation.id as string, 20, undefined, searchQuery);
        // Extract array from standard API response wrapper if present
        const resultsArray = Array.isArray((res as any).data) ? (res as any).data : res;
        setSearchResults(resultsArray as unknown as SearchResult[]);
      } catch (error) {
        console.error("Search failed:", error);
      } finally {
        setIsSearching(false);
      }
    }, 500);

    return () => {
      if (debounceTimeout.current) clearTimeout(debounceTimeout.current);
    };
  }, [searchQuery, isSearchMode, activeConversation?.id]);

  // Resize logic
  const handleMouseDown = (e: React.MouseEvent) => {
    isResizing.current = true;
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none'; // Prevent text selection while dragging
  };

  const handleMouseMove = useCallback((e: MouseEvent) => {
    if (!isResizing.current) return;
    
    // Calculate new width from right edge of screen
    const newWidth = window.innerWidth - e.clientX;
    
    if (newWidth >= MIN_WIDTH && newWidth <= MAX_WIDTH) {
      setPanelWidth(newWidth);
    } else if (newWidth < MIN_WIDTH) {
      setPanelWidth(MIN_WIDTH);
    } else if (newWidth > MAX_WIDTH) {
      setPanelWidth(MAX_WIDTH);
    }
  }, []);

  const handleMouseUp = useCallback(() => {
    isResizing.current = false;
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
  }, []);

  useEffect(() => {
    document.addEventListener('mousemove', handleMouseMove);
    document.addEventListener('mouseup', handleMouseUp);
    return () => {
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
    };
  }, [handleMouseMove, handleMouseUp]);

  if (!showInfoPanel || !activeConversation) return null;
  console.log(searchResults)
  return (
    <div 
      className="relative flex flex-col h-full bg-surface/50 backdrop-blur-xl shrink-0 transition-none border-l border-glass-border overflow-hidden"
      style={{ width: `${panelWidth}px` }}
    >
      {/* Resizer Handle */}
      <div 
        className="absolute left-0 top-0 bottom-0 w-1.5 cursor-col-resize hover:bg-brand-primary/50 transition-colors z-10"
        onMouseDown={handleMouseDown}
      />

      <div className="flex flex-col w-full h-full overflow-y-auto custom-scrollbar">
        {isSearchMode ? (
          <>
            {/* Search Header */}
            <div className="h-[64px] flex items-center gap-3 px-4 border-b border-glass-border shrink-0">
              <button 
                onClick={() => setIsSearchMode(false)}
                className="p-2 rounded-full hover:bg-glass cursor-pointer text-gray-500 hover:text-text-primary transition-colors shrink-0"
              >
                <ArrowLeft size={20} />
              </button>
              <div className="flex-1 bg-surface-solid rounded-xl border border-glass-border flex items-center px-3 py-1.5 h-[36px]">
                <input 
                  autoFocus
                  placeholder="Tìm kiếm tin nhắn..."
                  className="bg-transparent border-none outline-none text-sm w-full text-text-primary placeholder:text-text-secondary"
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                />
                {searchQuery && (
                  <button onClick={() => setSearchQuery("")} className="text-gray-400 hover:text-text-primary ml-2 shrink-0">
                    <X size={16} />
                  </button>
                )}
              </div>
            </div>
            
            {/* Search Results */}
            <div className="flex-1 overflow-y-auto p-2 space-y-1">
              {isSearching && <div className="text-center text-sm text-text-secondary py-4">Đang tìm kiếm...</div>}
              {!isSearching && searchQuery && searchResults.length === 0 && (
                <div className="text-center text-sm text-text-secondary py-4">Không tìm thấy kết quả.</div>
              )}
              {!isSearching && searchResults.map((result) => (
                <div 
                  key={result.id}
                  onClick={() => {
                    setTargetMessageId(result.id);
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
        ) : mediaMode ? (
          <>
            {/* Media Header */}
            <div className="h-[64px] flex items-center gap-3 px-4 border-b border-glass-border shrink-0">
              <button 
                onClick={() => setMediaMode(null)}
                className="p-2 rounded-full hover:bg-glass cursor-pointer text-gray-500 hover:text-text-primary transition-colors shrink-0"
              >
                <ArrowLeft size={20} />
              </button>
              <h3 className="font-semibold text-lg text-text-primary">
                {mediaMode === 'image' ? 'Hình ảnh' : 'Video'}
              </h3>
            </div>
            
            {/* Media Grid */}
            <div className="flex-1 overflow-y-auto p-2">
              {isLoadingMedia && <div className="text-center text-sm text-text-secondary py-4">Đang tải...</div>}
              {!isLoadingMedia && mediaItems.length === 0 && (
                <div className="text-center text-sm text-text-secondary py-4">Chưa có {mediaMode === 'image' ? 'hình ảnh' : 'video'} nào.</div>
              )}
              {!isLoadingMedia && mediaItems.length > 0 && (
                <div className="grid grid-cols-3 gap-1">
                  {mediaItems.map((msg: any) => (
                    <div 
                      key={msg.id || msg._id} 
                      className="aspect-square relative cursor-pointer group rounded-sm overflow-hidden bg-surface-solid"
                      onClick={() => {
                        if (mediaMode === 'image' && msg.image?.url) {
                          setSelectedMedia({ url: msg.image.url, type: 'image' });
                        } else if (mediaMode === 'video' && msg.video?.url) {
                          setSelectedMedia({ url: msg.video.url, type: 'video' });
                        }
                      }}
                    >
                      {mediaMode === 'image' && msg.image?.url && (
                        <Image
                          src={msg.image.url}
                          alt="image"
                          fill
                          className="object-cover group-hover:scale-105 transition-transform duration-300"
                        />
                      )}
                      {mediaMode === 'video' && (msg.video?.thumbnail_url || msg.video?.url) && (
                        <>
                          <Image
                            src={msg.video.thumbnail_url || msg.video.url}
                            alt="video"
                            fill
                            className="object-cover group-hover:scale-105 transition-transform duration-300"
                          />
                          <div className="absolute inset-0 flex items-center justify-center bg-black/20 group-hover:bg-black/10 transition-colors">
                            <VideoIcon className="text-white drop-shadow-md" size={24} />
                          </div>
                        </>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </div>
          </>
        ) : (
          <>
            {/* Header */}
            <div className="h-[64px] flex items-center justify-between px-4 border-b border-glass-border shrink-0">
              <h3 className="font-semibold text-lg text-text-primary">Thông tin hội thoại</h3>
              <button
                onClick={toggleInfoPanel}
                className="p-2 rounded-full hover:bg-glass cursor-pointer text-gray-500 hover:text-text-primary transition-colors"
              >
                <X size={20} />
              </button>
            </div>

        {/* Profile Section */}
        <div className="flex flex-col items-center py-6 px-4 border-b border-glass-border shrink-0">
          <div className="relative mb-3">
            <Image
              src={displayAvatar}
              alt="avatar"
              width={80}
              height={80}
              className="rounded-full object-cover size-20 shadow-md ring-4 ring-surface"
            />
            {activeConversation.type === "group" && isOnline && (
              <div className="absolute bottom-1 right-1 size-4 rounded-full bg-green-500 border-2 border-surface"></div>
            )}
          </div>
          <h2 className="text-xl font-bold text-text-primary text-center leading-tight">
            {displayName}
          </h2>
          {activeConversation.type === "utu" && otherMember && (
            <div className="text-sm mt-1">
              {isOnline ? (
                <span className="text-green-500 font-medium">Đang hoạt động</span>
              ) : (
                <span className="text-gray-500">
                  {otherMember.last_active
                    ? `Hoạt động ${getRelativeTime(otherMember.last_active)}`
                    : "Ngoại tuyến"}
                </span>
              )}
            </div>
          )}
        </div>

        {/* Quick Actions */}
        <div className="flex flex-row justify-center gap-4 py-4 border-b border-glass-border shrink-0">
          <button 
            className="flex flex-col items-center gap-1 group cursor-pointer"
            onClick={() => setIsSearchMode(true)}
          >
            <div className="p-3 bg-glass rounded-full group-hover:bg-brand-primary/10 group-hover:text-brand-primary transition-colors text-text-secondary cursor-pointer">
              <Search size={20} />
            </div>
            <span className="text-[11px] text-text-secondary group-hover:text-brand-primary cursor-pointer">Tìm kiếm</span>
          </button>

          {activeConversation.type === "utu" && (
            <button 
              className="flex flex-col items-center gap-1 group cursor-pointer"
              onClick={() => setCreateGroupOpen(true)}
            >
              <div className="p-3 bg-glass rounded-full group-hover:bg-brand-primary/10 group-hover:text-brand-primary transition-colors text-text-secondary cursor-pointer">
                <UserPlus size={20} />
              </div>
              <span className="text-[11px] text-text-secondary group-hover:text-brand-primary">Tạo nhóm</span>
            </button>
          )}

          {(activeConversation.type === "utu" || (activeConversation.type === "group" && isAdmin)) && (
            <button 
              className="flex flex-col items-center gap-1 group cursor-pointer"
              onClick={() => setShowEditGroupModal(true)}
            >
              <div className="p-3 bg-glass rounded-full group-hover:bg-brand-primary/10 group-hover:text-brand-primary transition-colors text-text-secondary cursor-pointer">
                <Settings size={20} />
              </div>
              <span className="text-[11px] text-text-secondary group-hover:text-brand-primary">Chỉnh sửa</span>
            </button>
          )}
          <button className="flex flex-col items-center gap-1 group cursor-pointer">
            <div className="p-3 bg-glass rounded-full group-hover:bg-brand-primary/10 group-hover:text-brand-primary transition-colors text-text-secondary">
              <Bell size={20} />
            </div>
            <span className="text-[11px] text-text-secondary group-hover:text-brand-primary">Thông báo</span>
          </button>
          {activeConversation.type === "group" && (
            <button 
              className="flex flex-col items-center gap-1 group cursor-pointer"
              onClick={() => setCreateGroupOpen(true)}
            >
              <div className="p-3 bg-glass rounded-full group-hover:bg-brand-primary/10 group-hover:text-brand-primary transition-colors text-text-secondary cursor-pointer">
                <UserPlus size={20} />
              </div>
              <span className="text-[11px] text-text-secondary group-hover:text-brand-primary">Thêm</span>
            </button>
          )}
        </div>

        {/* Media Filters Section */}
        <div className="flex flex-col p-4 border-b border-glass-border shrink-0">
          <div 
            className="flex items-center justify-between mb-3 cursor-pointer group"
            onClick={() => setIsMediaExpanded(!isMediaExpanded)}
          >
            <h4 className="font-semibold text-sm text-text-secondary group-hover:text-text-primary transition-colors">
              File phương tiện, file và liên kết
            </h4>
            {isMediaExpanded ? (
              <ChevronDown size={18} className="text-text-secondary transition-transform" />
            ) : (
              <ChevronRight size={18} className="text-text-secondary transition-transform" />
            )}
          </div>
          
          <div className={`flex gap-2 overflow-hidden transition-all duration-300 ${isMediaExpanded ? 'max-h-[100px] opacity-100' : 'max-h-0 opacity-0'}`}>
            <button 
              onClick={() => setMediaMode('image')}
              className="flex-1 flex flex-col items-center gap-1.5 p-2 rounded-xl hover:bg-glass-panel transition-colors cursor-pointer group"
            >
              <div className="p-2.5 bg-blue-500/10 text-blue-500 rounded-lg group-hover:bg-blue-500/20 transition-colors">
                <ImageIcon size={20} />
              </div>
              <span className="text-xs font-medium text-text-secondary group-hover:text-text-primary">Ảnh</span>
            </button>

            <button 
              onClick={() => setMediaMode('video')}
              className="flex-1 flex flex-col items-center gap-1.5 p-2 rounded-xl hover:bg-glass-panel transition-colors cursor-pointer group"
            >
              <div className="p-2.5 bg-purple-500/10 text-purple-500 rounded-lg group-hover:bg-purple-500/20 transition-colors">
                <VideoIcon size={20} />
              </div>
              <span className="text-xs font-medium text-text-secondary group-hover:text-text-primary">Video</span>
            </button>

            <button 
              disabled
              className="flex-1 flex flex-col items-center gap-1.5 p-2 rounded-xl hover:bg-glass-panel transition-colors cursor-not-allowed group opacity-50"
              title="Đang phát triển"
            >
              <div className="p-2.5 bg-green-500/10 text-green-500 rounded-lg">
                <FileIcon size={20} />
              </div>
              <span className="text-xs font-medium text-text-secondary">File</span>
            </button>
          </div>
        </div>

        {/* Group Members Section */}
        {activeConversation.type === "group" && (
          <div className="flex flex-col p-4 border-b border-glass-border shrink-0">
            <div 
              className="flex items-center justify-between mb-3 cursor-pointer group"
              onClick={() => setIsMembersExpanded(!isMembersExpanded)}
            >
              <h4 className="font-semibold text-sm text-text-secondary group-hover:text-text-primary transition-colors">
                Thành viên đoạn chat ({activeConversation.member_ids?.length || 0})
              </h4>
              {isMembersExpanded ? (
                <ChevronDown size={18} className="text-text-secondary transition-transform" />
              ) : (
                <ChevronRight size={18} className="text-text-secondary transition-transform" />
              )}
            </div>
            
            <div className={`flex flex-col gap-3 overflow-hidden transition-all duration-300 ${isMembersExpanded ? 'max-h-[300px] overflow-y-auto custom-scrollbar' : 'max-h-0'}`}>
              {activeConversation.member_ids?.map((memberId) => {
                const member = users[memberId];
                const isMe = memberId === user?.id;
                const name = isMe ? "Bạn" : (member?.name || "Người dùng");
                const avatar = member?.avatar_url || DEFAULT_AVATAR;
                
                return (
                  <div key={memberId} className="flex items-center gap-3">
                    <div 
                      className="relative cursor-pointer hover:opacity-80 transition-opacity"
                      onClick={() => openUserProfileModal(memberId)}
                    >
                      <Image
                        src={avatar}
                        alt="avatar"
                        width={36}
                        height={36}
                        className="rounded-full object-cover size-9 shrink-0"
                      />
                      {!isMe && member?.is_online && (
                        <div className="absolute bottom-0 right-0 size-2.5 rounded-full bg-green-500 border-2 border-surface"></div>
                      )}
                    </div>
                    <div className="flex flex-col min-w-0">
                      <span className="text-sm font-medium text-text-primary truncate">
                        {name}
                      </span>
                      {activeConversation.admin_ids?.includes(memberId) && (
                        <span className="text-[10px] bg-brand-primary/10 text-brand-primary px-1.5 py-0.5 rounded w-fit">
                          Quản trị viên
                        </span>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {/* Settings / Danger Zone */}
        <div className="flex flex-col mt-auto p-4 gap-2 shrink-0">
          
          {activeConversation.type === "group" && isAdmin && (
            <>
              <button
                onClick={() => setAssignAdminModalOpen(true)}
                className="flex items-center gap-3 p-3 rounded-xl hover:bg-blue-500/10 text-blue-500 transition-colors w-full cursor-pointer"
              >
                <ShieldCheck size={20} />
                <span className="font-medium text-sm">Cấp quyền Admin</span>
              </button>
              <button
                onClick={() => setKickModalOpen(true)}
                className="flex items-center gap-3 p-3 rounded-xl hover:bg-orange-500/10 text-orange-500 transition-colors w-full cursor-pointer"
              >
                <UserMinus size={20} />
                <span className="font-medium text-sm">Xóa thành viên</span>
              </button>
            </>
          )}

          {activeConversation.type === "group" && (
            <>
              <button
                onClick={handleLeaveGroup}
                className="flex items-center gap-3 p-3 rounded-xl hover:bg-hover text-txt-primary transition-colors w-full cursor-pointer"
              >
                <LogOut size={20} />
                <span className="font-medium text-sm">Rời khỏi nhóm</span>
              </button>

              {isAdmin && (
                <button
                  onClick={() => setShowDisbandModal(true)}
                  disabled={isDisbanding}
                  className="flex items-center gap-3 p-3 rounded-xl hover:bg-red-500/10 text-red-500 transition-colors w-full cursor-pointer disabled:opacity-50"
                >
                  <Trash2 size={20} />
                  <span className="font-medium text-sm">
                    {isDisbanding ? "Đang xử lý..." : "Giải tán nhóm"}
                  </span>
                </button>
              )}
            </>
          )}

          {/* 1-on-1 Block / Unblock */}
          {activeConversation.type === "utu" && (
            <>
              {activeConversation.block ? (
                activeConversation.block.block_by === user?.id && (
                  <button
                    onClick={handleUnblockUser}
                    disabled={isBlocking}
                    className="flex items-center gap-3 p-3 rounded-xl hover:bg-brand-primary/10 text-brand-primary transition-colors w-full cursor-pointer disabled:opacity-50"
                  >
                    <ShieldCheck size={20} />
                    <span className="font-medium text-sm">
                      {isBlocking ? "Đang xử lý..." : "Bỏ chặn người dùng"}
                    </span>
                  </button>
                )
              ) : (
                <button
                  onClick={() => setShowBlockModal(true)}
                  disabled={isBlocking}
                  className="flex items-center gap-3 p-3 rounded-xl hover:bg-red-500/10 text-red-500 transition-colors w-full cursor-pointer disabled:opacity-50"
                >
                  <Ban size={20} />
                  <span className="font-medium text-sm">
                    {isBlocking ? "Đang xử lý..." : "Chặn người dùng"}
                  </span>
                </button>
              )}
            </>
          )}
        </div>
        </>
        )}
      </div>

      <MediaViewerModal 
        isOpen={!!selectedMedia}
        onClose={() => setSelectedMedia(null)}
        mediaType={selectedMedia?.type || 'image'}
        mediaUrl={selectedMedia?.url || ''}
      />

      <EditGroupModal 
        isOpen={showEditGroupModal}
        onClose={() => setShowEditGroupModal(false)}
        conversation={activeConversation}
        onSuccess={(updatedConv) => {
          // Sync with chatStore
          const { setActiveConversation } = useChatStore.getState();
          setActiveConversation(updatedConv);

          // Update React Query cache for sidebar
          const updateQueryCache = (queryKey: any[]) => {
            queryClient.setQueryData(queryKey, (oldData: any) => {
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
        isOpen={showBlockModal}
        onClose={() => setShowBlockModal(false)}
        onConfirm={handleBlockUser}
        userName={displayName}
        userAvatar={displayAvatar}
        userEmail={otherMember?.email}
        isSubmitting={isBlocking}
      />

      <DisbandGroupModal
        isOpen={showDisbandModal}
        onClose={() => setShowDisbandModal(false)}
        onConfirm={handleDisbandGroup}
        groupName={displayName}
        groupAvatar={displayAvatar}
        memberCount={activeConversation?.member_ids?.length || 0}
        isSubmitting={isDisbanding}
      />
    </div>
  );
};

export default ConversationInfo;
