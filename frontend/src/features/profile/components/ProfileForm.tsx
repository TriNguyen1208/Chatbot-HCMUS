import React, { useState } from 'react';
import Image from 'next/image';
import { useProfileForm } from '../hooks/useProfileForm';
import { Camera, Loader2, Save, ArrowLeft, Maximize, X } from 'lucide-react';
import { DEFAULT_AVATAR } from '@/utils/constants';
import Link from 'next/link';

export const ProfileForm = () => {
  const {
    currentUser,
    phone,
    setPhone,
    avatarUrl,
    isUploading,
    isSaving,
    fileInputRef,
    handleAvatarClick,
    handleFileChange,
    handleSave,
    handleCancel
  } = useProfileForm();

  const [showImageModal, setShowImageModal] = useState(false);

  if (!currentUser) {
    return (
      <div className="flex justify-center items-center h-screen bg-bg-primary">
        <Loader2 className="animate-spin text-ic-primary" size={32} />
      </div>
    );
  }

  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-bg-primary p-4 sm:p-8 relative overflow-hidden">
      {/* Decorative background blobs (glassmorphism effect) */}
      <div className="absolute top-[-10%] left-[-10%] w-[40%] h-[40%] rounded-full bg-blue-500/20 blur-[100px] pointer-events-none" />
      <div className="absolute bottom-[-10%] right-[-10%] w-[40%] h-[40%] rounded-full bg-purple-500/20 blur-[100px] pointer-events-none" />

      <div className="w-full max-w-2xl bg-surface/50 backdrop-blur-2xl border border-glass-border rounded-3xl p-6 sm:p-10 shadow-2xl z-10">
        
        {/* Header */}
        <div className="flex items-center gap-4 mb-8">
          <button 
            onClick={handleCancel}
            className="p-2 bg-surface hover:bg-hover rounded-xl border border-glass-border transition-colors group"
            title="Go back"
          >
            <ArrowLeft size={20} className="text-txt-primary group-hover:-translate-x-1 transition-transform" />
          </button>
          <h1 className="text-3xl font-bold text-txt-primary tracking-tight">Personal Profile</h1>
        </div>

        <form onSubmit={handleSave} className="flex flex-col gap-8">
          
          {/* Avatar Section */}
          <div className="flex flex-col items-center sm:items-start sm:flex-row gap-6 p-6 bg-surface/30 rounded-2xl border border-glass-border">
            <div className="relative group cursor-pointer" onClick={() => setShowImageModal(true)}>
              <div className={`w-32 h-32 rounded-full overflow-hidden border-4 border-glass-border shadow-lg relative ${isUploading ? 'opacity-50' : ''}`}>
                <Image
                  src={avatarUrl || DEFAULT_AVATAR}
                  alt={currentUser.name || "User Avatar"}
                  fill
                  className="object-cover"
                />
                
                {/* Overlay on hover */}
                <div className="absolute inset-0 bg-black/40 flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity duration-300">
                  <Maximize className="text-white" size={28} />
                </div>
              </div>
              
              {isUploading && (
                <div className="absolute inset-0 flex items-center justify-center">
                  <Loader2 className="animate-spin text-ic-primary bg-surface/80 rounded-full p-1" size={32} />
                </div>
              )}
            </div>
            {/* Hidden file input */}
            <input 
              type="file" 
              ref={fileInputRef} 
              onChange={handleFileChange} 
              accept="image/*" 
              className="hidden" 
            />
            
            <div className="flex flex-col justify-center gap-2 text-center sm:text-left">
              <h2 className="text-xl font-semibold text-txt-primary">{currentUser.name}</h2>
              <p className="text-sm text-txt-extra">Update your photo and personal details here.</p>
              <button 
                type="button"
                onClick={handleAvatarClick}
                disabled={isUploading}
                className="mt-2 text-sm px-4 py-2 bg-surface hover:bg-hover border border-glass-border rounded-xl text-txt-primary transition-colors flex items-center gap-2 justify-center sm:justify-start w-max mx-auto sm:mx-0"
              >
                <Camera size={16} />
                {isUploading ? 'Uploading...' : 'Change Avatar'}
              </button>
            </div>
          </div>

          {/* Form Fields */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="flex flex-col gap-2">
              <label className="text-sm font-medium text-txt-secondary ml-1">Full Name</label>
              <input 
                type="text" 
                value={currentUser.name || ""} 
                disabled 
                className="w-full px-4 py-3 bg-surface/40 border border-glass-border rounded-xl text-txt-secondary cursor-not-allowed focus:outline-none"
              />
            </div>

            <div className="flex flex-col gap-2">
              <label className="text-sm font-medium text-txt-secondary ml-1">Student ID (MSSV)</label>
              <input 
                type="text" 
                value={currentUser.studentID || "N/A"} 
                disabled 
                className="w-full px-4 py-3 bg-surface/40 border border-glass-border rounded-xl text-txt-secondary cursor-not-allowed focus:outline-none"
              />
            </div>

            <div className="flex flex-col gap-2">
              <label className="text-sm font-medium text-txt-secondary ml-1">Email Address</label>
              <input 
                type="email" 
                value={currentUser.email || ""} 
                disabled 
                className="w-full px-4 py-3 bg-surface/40 border border-glass-border rounded-xl text-txt-secondary cursor-not-allowed focus:outline-none"
              />
            </div>

            <div className="flex flex-col gap-2">
              <label className="text-sm font-medium text-txt-secondary ml-1">Phone Number</label>
              <input 
                type="tel" 
                value={phone} 
                onChange={(e) => setPhone(e.target.value)}
                placeholder="Enter your phone number"
                className="w-full px-4 py-3 bg-surface/70 border border-glass-border rounded-xl text-txt-primary focus:outline-none focus:ring-2 focus:ring-blue-500/50 transition-all placeholder:text-txt-extra"
              />
            </div>
          </div>

          {/* Actions */}
          <div className="flex justify-end pt-4 border-t border-glass-border mt-2 gap-4">
            <button 
              type="button"
              onClick={handleCancel}
              className="px-6 py-3 bg-surface hover:bg-hover border border-glass-border rounded-xl text-txt-primary font-medium transition-colors"
            >
              Cancel
            </button>
            <button 
              type="submit"
              disabled={isUploading || isSaving}
              className="px-6 py-3 bg-blue-600 hover:bg-blue-700 text-white rounded-xl font-medium transition-all shadow-lg hover:shadow-blue-500/25 flex items-center gap-2 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {isSaving ? <Loader2 className="animate-spin" size={18} /> : <Save size={18} />}
              Save Changes
            </button>
          </div>

        </form>
      </div>

      {showImageModal && (
        <div 
          className="fixed inset-0 z-[100] flex items-center justify-center bg-black/80 backdrop-blur-sm cursor-zoom-out"
          onClick={() => setShowImageModal(false)}
        >
          <img 
            src={avatarUrl || DEFAULT_AVATAR} 
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
};
