"use client";
import { Plus, Image as ImageIcon, Smile, SendHorizontal, X, Loader2 } from "lucide-react";
import { useChatInput } from "@/features/chat/hooks/useChatInput";

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
    handleKeyDown,
    handleFileClick,
    handleFileChange,
    removeSelectedFile
  } = useChatInput();

  return (
    <div className="w-full flex flex-col px-4 pb-6 pt-2 bg-transparent">
      <div className="w-full max-w-5xl mx-auto flex flex-col p-3 bg-surface backdrop-blur-xl border border-glass-border shadow-lg rounded-[2rem] transition-all duration-300">
        <div className="flex flex-row items-center gap-2 mb-2 px-2">
          <button className="flex items-center gap-1 text-[11px] font-medium border border-glass-border rounded-full px-3 py-1 bg-surface-solid shadow-sm hover:shadow text-txt-primary transition-shadow">
            ✨ Suggest Reply
          </button>
          <button className="flex items-center gap-1 text-[11px] font-medium border border-glass-border rounded-full px-3 py-1 bg-surface-solid shadow-sm hover:shadow text-txt-primary transition-shadow">
            📄 Summarize PDF
          </button>
        </div>

        {(previewUrl || isPreviewLoading) && (
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
                className="absolute -top-2 -right-2 bg-surface-solid rounded-full p-1 shadow-md border border-glass-border hover:bg-hover transition-colors"
              >
                <X size={14} className="text-txt-extra" />
              </button>
            )}
          </div>
        )}

        <div className="flex flex-row items-center bg-input-surface rounded-full px-4 py-2 gap-3 w-full border border-glass-border shadow-inner">
          <button className="text-ic-primary hover:text-brand-primary transition-colors"><Plus size={20} /></button>
          <button className="text-ic-primary hover:text-brand-primary transition-colors" onClick={handleFileClick}><ImageIcon size={20} /></button>
          <input 
            type="file" 
            className="hidden" 
            ref={fileInputRef} 
            onChange={handleFileChange} 
            accept="image/*,video/*" 
          />
          <input 
            type="text" 
            value={content}
            onChange={(e) => setContent(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Type a message..." 
            className="flex-1 bg-transparent outline-none text-sm text-txt-primary placeholder:text-txt-extra"
          />
          <button className="text-ic-primary hover:text-brand-primary transition-colors"><Smile size={20} /></button>
          <button 
            onClick={handleSend}
            className="bg-gradient-primary text-white p-2 rounded-full shadow-md hover:shadow-lg disabled:opacity-50 disabled:shadow-none flex items-center justify-center transition-all duration-300 hover:scale-105 active:scale-95"
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
