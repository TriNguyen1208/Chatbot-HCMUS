import { format } from "date-fns";
import { useAuthStore } from "@/features/auth/stores/authStore";
import { Message } from "../types";
import { useState, useRef, useEffect } from "react";
import { messageApi } from "../api/message.api";

export const useMessageItem = (message: Message) => {
  const { user } = useAuthStore();

  const senderId = message.sender?.id;
  const isMe = senderId === user?.id;

  const isSystem = message.type === 'system' || senderId === 'system';

  const timeDisplay = message.created_at ? format(new Date(message.created_at), "h:mm a") : "";

  const [showMenu, setShowMenu] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);

  const [showReactMenu, setShowReactMenu] = useState(false);
  const reactMenuRef = useRef<HTMLDivElement>(null);

  const [showReactionList, setShowReactionList] = useState(false);

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(event.target as Node)) {
        setShowMenu(false);
      }
      if (reactMenuRef.current && !reactMenuRef.current.contains(event.target as Node)) {
        setShowReactMenu(false);
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  const handleRecall = async () => {
    try {
      await messageApi.recallMessage(message._id || (message as any).id);
      setShowMenu(false);
    } catch (error) {
      console.error("Failed to recall message", error);
    }
  };

  const handleForward = () => {
    console.log("Forward message", message._id);
    setShowMenu(false);
  };

  const handleReact = async (emoji: string) => {
    setShowReactMenu(false);
    try {
      await messageApi.toggleReaction(message._id || (message as any).id, emoji);
    } catch (error) {
      console.error("Failed to toggle reaction", error);
    }
  };

  return {
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
  };
};
