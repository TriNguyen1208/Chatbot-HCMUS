"use client";
import { Plus, Image as ImageIcon, Smile, SendHorizontal, X, Loader2, Ban, ShieldAlert } from "lucide-react";
import data from '@emoji-mart/data';
import Picker from '@emoji-mart/react';
import { useChatInput } from "./useChatInput";

const ChatInput = () => {
  const {
    content,
    setContent,
    fileInputRef,
    uploadedMedia,
    previewUrl,
    isPreviewLoading,
    isUploading,
    handleSend,
    handleSendPrimaryIcon,
    handleKeyDown,
    handleFileClick,
    handleFileChange,
    removeSelectedFile,
    emojiPickerRef,
    showEmojiPicker,
    setShowEmojiPicker,
    handleEmojiSelect,
    editingMessage,
    cancelEdit,
    isBlocked,
    isBlocker,
    blockerName,
    blockTimeDisplay,
    handleUnblock,
    isUnblocking,
    primaryIcon
  } = useChatInput();

  if (isBlocked) {

    return (
      <div className="w-full flex flex-col px-4 pb-6 pt-2 bg-transparent">
        <div className="w-full max-w-3xl mx-auto flex items-center justify-between gap-4 p-4 bg-surface/90 backdrop-blur-xl border border-glass-border shadow-lg rounded-2xl animate-in fade-in duration-200">
          <div className="flex items-center gap-3 min-w-0">
            <div className={`p-2.5 rounded-xl shrink-0 ${isBlocker ? "bg-red-500/10 text-red-500 border border-red-500/20" : "bg-orange-500/10 text-orange-500 border border-orange-500/20"}`}>
              {isBlocker ? <Ban size={22} /> : <ShieldAlert size={22} />}
            </div>
            <div className="flex flex-col min-w-0">
              <span className="text-sm font-semibold text-txt-primary truncate">
                {isBlocker
                  ? "Bạn đã chặn cuộc trò chuyện này"
                  : `${blockerName} đã chặn bạn`}
              </span>
              <span className="text-xs text-txt-extra mt-0.5">
                {isBlocker
                  ? `Chặn vào lúc ${blockTimeDisplay}. Bỏ chặn để tiếp tục nhắn tin.`
                  : `Vào lúc ${blockTimeDisplay}. Bạn không thể gửi tin nhắn trong cuộc trò chuyện này.`}
              </span>
            </div>
          </div>

          {isBlocker && (
            <button
              onClick={handleUnblock}
              disabled={isUnblocking}
              className="px-4 py-2 text-sm font-medium text-white bg-gradient-primary rounded-xl shadow hover:shadow-md hover:-translate-y-0.5 transition-all cursor-pointer shrink-0 disabled:opacity-50"
            >
              {isUnblocking ? "Đang xử lý..." : "Bỏ chặn"}
            </button>
          )}
        </div>
      </div>
    );
  }

  return (
    <div className="w-full flex flex-col px-4 pb-6 pt-2 bg-transparent">
      <div className="w-full max-w-5xl mx-auto flex flex-col p-3 bg-surface backdrop-blur-xl border border-glass-border shadow-lg rounded-[2rem] transition-all duration-300">
        {!editingMessage && (
          <div className="flex flex-row items-center gap-2 mb-2 px-2">
            <button className="flex items-center gap-1 text-[11px] font-medium border border-glass-border rounded-full px-3 py-1 bg-surface-solid shadow-sm hover:shadow text-txt-primary transition-shadow cursor-pointer">
              ✨ Suggest Reply
            </button>
            <button className="flex items-center gap-1 text-[11px] font-medium border border-glass-border rounded-full px-3 py-1 bg-surface-solid shadow-sm hover:shadow text-txt-primary transition-shadow cursor-pointer">
              📄 Summarize PDF
            </button>
          </div>
        )}

        {editingMessage && (
          <div className="flex items-center justify-between bg-brand-primary/10 px-4 py-2 rounded-xl mb-2 border border-brand-primary/20">
            <div className="flex flex-col">
              <span className="text-xs font-semibold text-brand-primary">Đang chỉnh sửa tin nhắn</span>
              <span className="text-xs text-txt-extra truncate max-w-md">{editingMessage.content}</span>
            </div>
            <button onClick={cancelEdit} className="p-1 rounded-full hover:bg-black/5 text-txt-extra hover:text-txt-primary transition-colors cursor-pointer">
              <X size={16} />
            </button>
          </div>
        )}

        {previewUrl && !editingMessage && (
          <div className="relative w-20 h-20 mb-3 ml-4">
            {isPreviewLoading ? (
              <div className="w-full h-full flex items-center justify-center bg-secondary rounded-xl border border-glass-border">
                 <Loader2 size={24} className="animate-spin text-ic-primary" />
              </div>
            ) : previewUrl ? (
               uploadedMedia?.type === 'video' ? (
                <video src={previewUrl} className="w-full h-full object-cover rounded-xl border border-glass-border shadow-sm" />
              ) : (
                <img src={previewUrl} alt="Preview" className="w-full h-full object-cover rounded-xl border border-glass-border shadow-sm" />
              )
            ) : null}
            
            {!isPreviewLoading && (
              <button 
                onClick={removeSelectedFile}
                className="absolute -top-2 -right-2 bg-surface-solid rounded-full p-1 shadow-md border border-glass-border hover:bg-hover transition-colors cursor-pointer"
              >
                <X size={14} className="text-txt-extra" />
              </button>
            )}
          </div>
        )}

        <div className="flex flex-row items-center bg-input-surface rounded-full px-4 py-2 gap-3 w-full border border-glass-border shadow-inner">
          {!editingMessage && (
            <>
              <button className="text-ic-primary hover:text-brand-primary transition-colors cursor-pointer"><Plus size={20} /></button>
              <button className="text-ic-primary hover:text-brand-primary transition-colors cursor-pointer" onClick={handleFileClick}><ImageIcon size={20} /></button>
              <input 
                type="file" 
                className="hidden" 
                ref={fileInputRef} 
                onChange={handleFileChange} 
                accept="image/*,video/*" 
              />
            </>
          )}
          <input 
            type="text" 
            value={content}
            onChange={(e) => setContent(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder={editingMessage ? "Nhập nội dung mới..." : "Type a message..."}
            className="flex-1 bg-transparent outline-none text-sm text-txt-primary placeholder:text-txt-extra"
          />
          <div className="relative" ref={emojiPickerRef}>
            <button 
              type="button"
              onClick={() => setShowEmojiPicker(!showEmojiPicker)}
              className="text-ic-primary hover:text-brand-primary transition-colors flex items-center justify-center h-full cursor-pointer"
            >
              <Smile size={20} />
            </button>
            
            {showEmojiPicker && (
              <div className="absolute bottom-10 right-0 z-50 shadow-2xl rounded-2xl overflow-hidden border border-glass-border">
                <Picker 
                  data={data} 
                  onEmojiSelect={handleEmojiSelect}
                  theme="light"
                  previewPosition="none"
                  skinTonePosition="none"
                />
              </div>
            )}
          </div>
          {!editingMessage && (
            <button
              type="button"
              onClick={handleSendPrimaryIcon}
              disabled={isUploading || isPreviewLoading}
              className="text-xl p-1 hover:scale-125 active:scale-95 transition-transform flex items-center justify-center cursor-pointer select-none disabled:opacity-50"
              title={`Gửi nhanh ${primaryIcon}`}
            >
              <span>{primaryIcon}</span>
            </button>
          )}
          <button 
            onClick={handleSend}
            className="bg-gradient-primary text-white p-2 rounded-full shadow-md hover:shadow-lg disabled:opacity-50 disabled:shadow-none flex items-center justify-center transition-all duration-300 hover:scale-105 active:scale-95 cursor-pointer"
            disabled={(!content.trim() && !uploadedMedia) || isUploading || isPreviewLoading}
          >
            {isUploading ? <Loader2 size={18} className="animate-spin" /> : <SendHorizontal size={18} />}
          </button>
        </div>
      </div>
      <div className="w-full text-center mt-3">
        <span className="text-[10px] text-txt-extra/70 font-medium tracking-wide">Protected by HCMUS AI Secure Workspace</span>
      </div>
    </div>
  );
};

export default ChatInput;
