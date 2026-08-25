import { Server as HttpServer } from "http";
import { Server, Socket } from "socket.io";

import { socketAuthMiddleware } from "#@/shared/middlewares/socket-auth.middleware.js";
import { config } from "#@/config/config.js";
import type { ConversationService } from "#@/modules/conversation/services/conversation.service.js";
import type { UserService } from "#@/modules/user/services/user.service.js";

const USER_ROOM = (userId: string) => `user:${userId}`;

const CONVERSATION_ROOM = (conversationId: string) =>
    `conversation:${conversationId}`;

export class SocketManager {
    private readonly io: Server;

    constructor(
        server: HttpServer,
        private readonly conversationService: ConversationService,
        private readonly userService: UserService,
    ) {
        this.io = new Server(server, {
            cors: {
                origin: config.corsOrigins,
                credentials: true,
            },
        });

        // Authentication middleware
        this.io.use(socketAuthMiddleware);

        this.registerConnectionHandler();
    }


    /**
     * Register all Socket.IO connection handling.
     */
    private registerConnectionHandler(): void {
        this.io.on("connection", (socket: Socket) => {
            const userId = socket.data.userId as string | undefined;

            if (!userId) {
                console.warn(
                    `[Socket.IO] Socket ${socket.id} connected without userId`
                );

                socket.disconnect(true);
                return;
            }
            socket.join(USER_ROOM(userId));

            console.log(
                `[Socket.IO] User '${userId}' connected with socket '${socket.id}'`
            );

            /*
            void this.userService.userConnect(userId, socket.id).then(async (isOnline) => {
                if (isOnline) {
                    try {
                        const conversations = await this.conversationService.getConversationList(userId, 100, undefined, 'utu');
                        const friends = new Set<string>();
                        conversations.forEach(c => {
                            c.member_ids?.forEach((mId: any) => {
                                if (mId.toString() !== userId) friends.add(mId.toString());
                            });
                        });
                        
                        if (friends.size > 0) {
                            this.emitToUsers(Array.from(friends), "user_status_changed", { userId, isOnline: true });
                        }
                    } catch (err) {
                        console.error(`[Socket.IO] Error broadcasting online status for user ${userId}`, err);
                    }
                }
            });
            */

            // Heartbeat
            /*
            socket.on("ping", () => {
                void this.userService.userHeartbeat(userId);
            });
            */

            // Typing Indicator Handlers
            socket.on("typing", async (data: { conversationId: string, name?: string }) => {
                if (!data?.conversationId) return;
                const room = CONVERSATION_ROOM(data.conversationId);
                if (!socket.rooms.has(room)) {
                    try {
                        const conv = await this.conversationService.getConversationById(data.conversationId, userId as string);
                        if (conv && conv.member_ids) {
                            conv.member_ids.forEach((mId: any) => this.joinGroup(mId.toString(), data.conversationId));
                        }
                    } catch (e) {
                        console.error("[Socket.IO] Error lazy joining on typing", e);
                    }
                }
                socket.to(room).emit("user_typing", {
                    conversationId: data.conversationId,
                    userId,
                    name: data.name || "Ai đó"
                });
            });

            socket.on("stop_typing", async (data: { conversationId: string }) => {
                if (!data?.conversationId) return;
                const room = CONVERSATION_ROOM(data.conversationId);
                if (!socket.rooms.has(room)) {
                    try {
                        const conv = await this.conversationService.getConversationById(data.conversationId, userId as string);
                        if (conv && conv.member_ids) {
                            conv.member_ids.forEach((mId: any) => this.joinGroup(mId.toString(), data.conversationId));
                        }
                    } catch (e) {
                        console.error("[Socket.IO] Error lazy joining on stop_typing", e);
                    }
                }
                socket.to(room).emit("user_stop_typing", {
                    conversationId: data.conversationId,
                    userId
                });
            });

            // Disconnect handler
            this.registerDisconnectHandler(socket, userId);
        });
    }





    /**
     * Handle socket disconnect.
     */
    private registerDisconnectHandler(
        socket: Socket,
        userId: string,
    ): void {
        socket.on("disconnect", (reason) => {
            console.log(
                `[Socket.IO] User '${userId}' disconnected. ` +
                `Socket: '${socket.id}', reason: '${reason}'`
            );
            
            /*
            void this.userService.userDisconnect(userId, socket.id).then(async justWentOffline => {
                if (justWentOffline) {
                    try {
                        const conversations = await this.conversationService.getConversationList(userId, 100, undefined, 'utu');
                        const lastActive = new Date().toISOString();
                        const friends = new Set<string>();
                        
                        conversations.forEach(c => {
                            c.member_ids?.forEach((mId: any) => {
                                if (mId.toString() !== userId) friends.add(mId.toString());
                            });
                        });
                        
                        if (friends.size > 0) {
                            this.emitToUsers(Array.from(friends), "user_status_changed", { userId, isOnline: false, lastActive });
                        }
                    } catch (err) {
                        console.error(`[Socket.IO] Error broadcasting offline status for user ${userId}`, err);
                    }
                }
            });
            */
        });
    }


    /**
     * Emit event to all active devices/tabs of a user.
     */
    public emitToUser(
        userId: string,
        event: string,
        data: unknown,
    ): void {
        // 🔥 CHANGED:
        // Không loop qua socketIds nữa.
        //
        // Socket.IO sẽ emit tới tất cả socket trong user room.
        this.io
            .to(USER_ROOM(userId))
            .emit(event, data);
    }


    /**
     * Make all active sockets of a user join a conversation.
     *
     * Useful when:
     * - creating a new conversation
     * - adding a member to a group
     */
    public joinGroup(
        userId: string,
        conversationId: string,
    ): void {
        const room = CONVERSATION_ROOM(conversationId);

        // 🔥 CHANGED:
        //
        // `io.in(userRoom).socketsJoin(room)`
        //
        // means:
        //
        // tất cả socket của user
        //       ↓
        // join conversation room
        //
        // Không cần tìm từng socketId.
        this.io
            .in(USER_ROOM(userId))
            .socketsJoin(room);

        console.log(
            `[Socket.IO] User '${userId}' joined conversation '${conversationId}'`
        );
    }


    /**
     * Make all active sockets of a user leave a conversation.
     */
    public leaveGroup(
        userId: string,
        conversationId: string,
    ): void {
        const room = CONVERSATION_ROOM(conversationId);

        // 🔥 CHANGED:
        // Không cần loop socket IDs.
        this.io
            .in(USER_ROOM(userId))
            .socketsLeave(room);

        console.log(
            `[Socket.IO] User '${userId}' left conversation '${conversationId}'`
        );
    }


    /**
     * Emit event to all members currently connected
     * to a conversation.
     */
    public emitToGroup(
        conversationId: string,
        event: string,
        data: unknown,
    ): void {
        const room = CONVERSATION_ROOM(conversationId);

        // 🔥 CHANGED:
        // Không cần inspect room trước khi emit.
        //
        // Socket.IO sẽ tự xử lý:
        // - room không tồn tại
        // - room có 1 socket
        // - room có 1000 sockets
        this.io
            .to(room)
            .emit(event, data);
    }


    /**
     * Emit event to multiple users.
     */
    public emitToUsers(
        userIds: string[],
        event: string,
        data: unknown,
    ): void {
        for (const userId of userIds) {
            this.emitToUser(userId, event, data);
        }
    }

    public disconnectUser(userId: string): void {
        this.io
            .in(USER_ROOM(userId))
            .disconnectSockets(true);

        console.log(
            `[Socket.IO] Force disconnected user '${userId}'`
        );
    }

    public async isUserOnline(userId: string): Promise<boolean> {
        const sockets = await this.io
            .in(USER_ROOM(userId))
            .fetchSockets();

        return sockets.length > 0;
    }


    /**
     * Get Socket.IO instance.
     *
     * Useful when some infrastructure-level operation
     * needs direct access.
     */
    public getIO(): Server {
        return this.io;
    }
}


// --------------------------------------------------
// Socket Manager initialization
// --------------------------------------------------

export let socketManager: SocketManager;


// 🔥 CHANGED:
// ConversationService được truyền vào thay vì dynamic import.
export const initSocket = (
    server: HttpServer,
    conversationService: ConversationService,
    userService: UserService,
): SocketManager => {
    socketManager = new SocketManager(
        server,
        conversationService,
        userService,
    );

    return socketManager;
};