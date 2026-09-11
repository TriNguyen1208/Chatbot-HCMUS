"use client";
import React from "react";
import Image from "next/image";
import { ChevronDown, ChevronRight, ArrowLeft, Image as ImageIcon, Video as VideoIcon, File as FileIcon } from "lucide-react";

export interface MediaGalleryProps {
  mediaMode: 'image' | 'video' | null;
  setMediaMode: (mode: 'image' | 'video' | null) => void;
  mediaItems: any[];
  isLoadingMedia: boolean;
  isMediaExpanded: boolean;
  setIsMediaExpanded: (expanded: boolean) => void;
  setSelectedMedia: (media: { url: string; type: 'image' | 'video' } | null) => void;
}

export const MediaGallery: React.FC<MediaGalleryProps> = ({
  mediaMode,
  setMediaMode,
  mediaItems,
  isLoadingMedia,
  isMediaExpanded,
  setIsMediaExpanded,
  setSelectedMedia,
}) => {
  // If user is inside a specific media mode (images/videos view)
  if (mediaMode) {
    return (
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
            <div className="text-center text-sm text-text-secondary py-4">
              Chưa có {mediaMode === 'image' ? 'hình ảnh' : 'video'} nào.
            </div>
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
    );
  }

  // Normal accordion mode
  return (
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
  );
};

export default MediaGallery;
