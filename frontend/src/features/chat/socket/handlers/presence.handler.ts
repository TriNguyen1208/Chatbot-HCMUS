import { Socket } from "socket.io-client";
import { useUserStore } from "../../stores/userStore";

export const registerPresenceHandlers = (socket: Socket) => {
  const onUserOnline = (data: { userId: string }) => {
    useUserStore.getState().updateUserPresence(data.userId, true);
  };

  const onUserOffline = (data: { userId: string; last_active: string }) => {
    useUserStore.getState().updateUserPresence(data.userId, false, data.last_active);
  };

  socket.on("user_online", onUserOnline);
  socket.on("user_offline", onUserOffline);

  return () => {
    socket.off("user_online", onUserOnline);
    socket.off("user_offline", onUserOffline);
  };
};
