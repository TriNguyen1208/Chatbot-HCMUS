import React, { useEffect, useState } from "react";
import { createPortal } from "react-dom";
import { X } from "lucide-react";

interface MediaViewerModalProps {
  isOpen: boolean;
  onClose: () => void;
  mediaType: "image" | "video";
  mediaUrl: string;
}

const MediaViewerModal: React.FC<MediaViewerModalProps> = ({ isOpen, onClose, mediaType, mediaUrl }) => {
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
  }, []);

  if (!isOpen || !mediaUrl || !mounted) return null;

  return createPortal(
    <div 
      className="fixed inset-0 z-[100] flex items-center justify-center bg-black/90 backdrop-blur-sm animate-in fade-in duration-200"
      onClick={onClose}
    >
      <div 
        className="relative max-w-6xl max-h-[90vh] w-full p-4 flex items-center justify-center" 
        onClick={(e) => e.stopPropagation()}
      >
        {mediaType === 'image' ? (
          <img 
            src={mediaUrl} 
            alt="Full screen media" 
            className="max-w-full max-h-[85vh] object-contain rounded-lg shadow-2xl" 
          />
        ) : (
          <video 
            src={mediaUrl} 
            controls 
            autoPlay 
            className="max-w-full max-h-[85vh] object-contain rounded-lg shadow-2xl bg-black" 
          />
        )}
        <button 
          onClick={onClose}
          className="absolute top-4 right-4 p-2 bg-black/50 hover:bg-black/80 text-white rounded-full transition-colors backdrop-blur-md z-10"
        >
          <X size={24} />
        </button>
      </div>
    </div>,
    document.body
  );
};

export default MediaViewerModal;
