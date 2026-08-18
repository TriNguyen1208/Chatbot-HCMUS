"use client";

import { useEffect } from "react";
import { useSocketContext } from "@/providers/SocketProvider";

export const useSocket = (
  event: string,
  callback: (...args: any[]) => void
) => {
  const { socket, isConnected } = useSocketContext();

  useEffect(() => {
    if (!socket || !isConnected) return;

    socket.on(event, callback);

    return () => {
      socket.off(event, callback);
    };
  }, [socket, isConnected, event, callback]);

  return { socket, isConnected };
};
