"use client";
import { DEFAULT_AVATAR } from "@/utils/constants";
import { Message } from "@/features/chat/types";
import { useEffect, useState } from "react";
import { useMessageItem } from "@/features/chat/hooks/useMessageItem";
import { useUserStore } from "@/features/chat/stores/userStore";
import Image from "next/image";
import { MoreVertical, CornerUpLeft, Forward, Smile, X, Check, CheckCheck } from "lucide-react";
import ReactionModal from "./ReactionModal";
import { useModalStore } from "@/features/chat/stores/modalStore";

interface MessageItemProps {
  message: Message;
  watermarks?: { type: 'delivered' | 'read', userId: string }[];
  isLastMessage?: boolean;
}

const MessageItem = ({ message, watermarks, isLastMessage = false }: MessageItemProps) => {
  const requestUser = useUserStore(state => state.requestUser);
  const senderUser = useUserStore(state => state.users[message.sender_id || '']);

  const [showImageModal, setShowImageModal] = useState(false);
  const [showDetails, setShowDetails] = useState(false);
  
  const shouldShowDetails = isLastMessage || showDetails;

  useEffect(() => {
    if (message.sender_id && !senderUser) {
      requestUser(message.sender_id);
    }
  }, [message.sender_id, senderUser, requestUser]);

  const { 
    isMe, 
    isSystem, 
    timeDisplay, 
    showMenu, 
    setShowMenu, 
    menuRef, 
    showReactMenu,
    setShowReactMenu,
    reactMenuRef,
    showReactionList,
    setShowReactionList,
    handleRecall, 
    handleForward,
    handleReact
  } = useMessageItem(message);

  if (isSystem) {
    return (
      <div className="flex flex-row justify-center w-full my-4">
        <div className="bg-surface backdrop-blur-sm text-txt-extra text-xs px-5 py-2 rounded-full max-w-[80%] text-center border border-glass-border shadow-sm flex items-center gap-2">
          <span>{message.content}</span>
        </div>
      </div>
    );
  }

  return (
    <div 
      id={`msg-${message.id || (message as any)._id}`} 
      className={`group flex flex-row w-full my-3 ${isMe ? 'justify-end' : 'justify-start'}`}
    >
      {!isMe && (
        <Image
          src={senderUser?.avatar_url || DEFAULT_AVATAR}
          alt="avatar"
          width={36}
          height={36}
          className="rounded-full object-cover shrink-0 size-9 mt-auto mr-3 shadow-sm cursor-pointer hover:opacity-80 transition-opacity"
          onClick={() => {
            if (message.sender_id) {
              useModalStore.getState().openUserProfileModal(message.sender_id);
            }
          }}
        />
      )}
      <div className={`max-w-[75%] flex flex-col relative ${isMe ? 'items-end' : 'items-start'}`}>
        
        {/* Hover Actions */}
        <div className={`absolute top-1/2 -translate-y-1/2 flex items-center gap-1 ${isMe ? '-left-[84px]' : '-right-[84px]'} opacity-0 group-hover:opacity-100 transition-opacity`}>
          
          <div className="relative">
            <button onClick={() => setShowReactMenu(!showReactMenu)} className="p-1.5 text-txt-extra hover:text-txt-primary rounded-full hover:bg-hover transition-colors">
              <Smile size={18} />
            </button>
            
            {showReactMenu && (
              <div ref={reactMenuRef} className={`absolute z-10 bottom-full ${isMe ? 'right-0' : 'left-0'} mb-2 bg-surface-solid border border-glass-border shadow-xl rounded-full px-3 py-2 flex flex-row items-center gap-2`}>
                {["❤️", "😆", "😮", "😢", "😡", "👍"].map(emoji => (
                  <button 
                    key={emoji} 
                    onClick={() => handleReact(emoji)}
                    className="text-2xl hover:scale-125 transition-transform origin-bottom"
                  >
                    {emoji}
                  </button>
                ))}
              </div>
            )}
          </div>

          <div className="relative">
            <button onClick={() => setShowMenu(!showMenu)} className="p-1.5 text-txt-extra hover:text-txt-primary rounded-full hover:bg-hover transition-colors">
              <MoreVertical size={18} />
            </button>
            
            {showMenu && (
              <div ref={menuRef} className={`absolute z-10 bottom-full ${isMe ? 'right-0' : 'left-0'} mb-1 w-40 bg-surface-solid border border-glass-border shadow-xl rounded-xl py-1.5 flex flex-col text-sm overflow-hidden`}>
                {isMe && message.status !== 'recalled' && (
                  <button onClick={handleRecall} className="flex items-center gap-2 px-3 py-2 text-red-500 hover:bg-red-500/10 text-left w-full transition-colors">
                    <CornerUpLeft size={16} /> Thu hồi
                  </button>
                )}
                <button onClick={handleForward} className="flex items-center gap-2 px-3 py-2 text-txt-primary hover:bg-hover text-left w-full transition-colors">
                  <Forward size={16} /> Chuyển tiếp
                </button>
              </div>
            )}
          </div>
        </div>

        <div 
          className={`flex flex-col gap-1.5 cursor-pointer ${isMe ? 'items-end' : 'items-start'}`}
          onClick={() => setShowDetails(!showDetails)}
        >
          {message.status === 'recalled' ? (
            <div className={`px-5 py-3 rounded-2xl border border-glass-border bg-transparent text-txt-extra italic shadow-sm backdrop-blur-sm`}>
              <p className="text-sm">Tin nhắn đã bị thu hồi</p>
            </div>
          ) : (
            <>
              {message.type === 'image' && message.image?.url && (
                <div 
                  className="rounded-2xl overflow-hidden border border-glass-border shadow-sm max-w-sm cursor-pointer"
                  onClick={(e) => {
                    e.stopPropagation();
                    setShowImageModal(true);
                  }}
                >
                  <img src={message.image.url} alt="Image message" className="w-full h-auto object-cover max-h-60 transition-transform hover:scale-105 duration-500" />
                </div>
              )}

              {message.type === 'video' && !message.video?.url && message.video?.file_key && (
                <div className="rounded-2xl overflow-hidden border border-glass-border shadow-sm max-w-sm bg-secondary flex flex-col items-center justify-center h-40 w-60">
                  <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-brand-primary mb-2"></div>
                  <span className="text-txt-extra text-sm animate-pulse">Processing Video...</span>
                </div>
              )}

              {message.type === 'video' && message.video?.url && (
                <div 
                  className="rounded-2xl overflow-hidden border border-glass-border shadow-sm max-w-sm bg-black"
                  onClick={(e) => e.stopPropagation()}
                >
                  <video src={message.video.url} poster={message.video.thumbnail_url} controls className="w-full h-auto max-h-60" />
                </div>
              )}

              {message.content && message.content.trim().length > 0 && (
                <div className={`px-5 py-3 rounded-[20px] shadow-sm transition-opacity hover:opacity-90 ${isMe ? 'bg-gradient-primary text-white rounded-br-sm' : 'bg-surface border border-glass-border text-txt-primary rounded-bl-sm backdrop-blur-sm'}`}>
                  <p className="text-[15px] leading-relaxed whitespace-pre-wrap break-words">{message.content}</p>
                </div>
              )}
            </>
          )}
        </div>
        
        {message.status !== 'recalled' && message.reactions && message.reactions.length > 0 && (
          <div className={`flex flex-wrap gap-1 mt-1 z-10 ${isMe ? 'justify-end mr-1' : 'justify-start ml-1'}`}>
            {Object.entries(
              message.reactions.reduce((acc, curr) => {
                acc[curr.emoji] = (acc[curr.emoji] || 0);
                return acc;
              }, {} as Record<string, number>)
            ).map(([emoji, count]) => (
              <button 
                key={emoji}
                onClick={() => setShowReactionList(true)}
                className="flex items-center gap-1 bg-surface-solid border border-glass-border px-1.5 py-0.5 rounded-full text-[11px] hover:bg-hover transition-colors shadow-sm"
              >
                <span>{emoji}</span>
                {count > 1 && <span className="text-txt-extra font-medium">{count}</span>}
              </button>
            ))}
          </div>
        )}

        {shouldShowDetails && (
          <div className={`flex flex-row items-center gap-1 mt-1.5 ${isMe ? 'justify-end mr-1' : 'justify-start ml-1'}`}>
            <span className="text-[11px] text-txt-extra/80 font-medium">{timeDisplay}</span>
          </div>
        )}
        
        {shouldShowDetails && isMe && (
          <div className="flex flex-row justify-end items-center gap-1 mt-1 mr-1">
            {(!watermarks || watermarks.length === 0) ? (
              <Check className="w-3.5 h-3.5 text-txt-extra/60" title="Đã gửi" />
            ) : (
              <>
                {watermarks.some(w => w.type === 'delivered') && !watermarks.some(w => w.type === 'read') && (
                  <CheckCheck className="w-3.5 h-3.5 text-brand-primary" title="Đã nhận" />
                )}
                {watermarks.filter(w => w.type === 'read').map((w, idx) => {
                  const wUser = useUserStore.getState().users[w.userId];
                  const avatar = wUser?.avatar_url || DEFAULT_AVATAR;
                  return (
                    <div key={`read-${idx}`} className="relative">
                      <Image
                        src={avatar}
                        alt="watermark"
                        width={14}
                        height={14}
                        className="rounded-full object-cover shadow-sm"
                        title={`${wUser?.name || 'Người dùng'} đã xem`}
                      />
                    </div>
                  );
                })}
              </>
            )}
          </div>
        )}
      </div>

      {showReactionList && <ReactionModal message={message} onClose={() => setShowReactionList(false)} />}
        {showImageModal && message.image?.url && (
        <div 
          className="fixed inset-0 z-[100] flex items-center justify-center bg-black/80 backdrop-blur-sm cursor-zoom-out"
          onClick={() => setShowImageModal(false)}
        >
          <img 
            src={message.image.url} 
            alt="Zoomed image" 
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
};

export default MessageItem;
