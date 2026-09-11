"use client";
import { useEffect } from "react";
import type { Message } from "@/types";
import { useUserStore } from "@/features/chat/stores/userStore";

export const useReactionModal = (message: Message) => {
  const { users, requestUser } = useUserStore();

  useEffect(() => {
    message.reactions?.forEach((reaction) => {
      if (reaction.user_id && !users[reaction.user_id]) {
        requestUser(reaction.user_id);
      }
    });
  }, [message.reactions, users, requestUser]);

  return { users };
};
