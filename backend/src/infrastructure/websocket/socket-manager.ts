import { Server as HttpServer } from "http";
import { Server, Socket } from "socket.io";

import { createAdapter } from "@socket.io/redis-adapter";
import { socketAuthMiddleware } from "#@/shared/middlewares/socket-auth.middleware.js";
import { config } from "#@/config/config.js";
import { redisClient } from "#@/infrastructure/redis/redis.js";
import { userContainer } from "#@/modules/user/user.container.js";

const USER_ROOM = (userId: string) => `user:${userId}`;
const ALL_USERS_ROOM = 'global:all_users';

const CONVERSATION_ROOM = (conversationId: string) =>
    `conversation:${conversationId}`;

export class SocketManager {
    private readonly io: Server;

    constructor(
        server: HttpServer
    ) {
        this.io = new Server(server, {
            cors: {
                origin: config.corsOrigins,
                credentials: true,
            },
        });
        
        // Setup Redis adapter for distributed sockets
        const pubClient = redisClient.getClient();
        const subClient = pubClient.duplicate();
        this.io.adapter(createAdapter(pubClient, subClient));

        // Authentication middleware
        this.io.use(socketAuthMiddleware);
        this.registerConnectionHandler();
    }

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
            socket.join(ALL_USERS_ROOM);

            console.log(
                `[Socket.IO] User '${userId}' connected with socket '${socket.id}'`
            );

            // Update presence in Redis (24h TTL)
            redisClient.set(`presence:${userId}`, "online", 24 * 3600).catch(err => {
                console.error("[Socket.IO] Failed to set redis presence", err);
            });

            // Broadcast to other users
            socket.broadcast.to(ALL_USERS_ROOM).emit("user_online", { userId });

            // Typing Indicator Handlers
            socket.on("typing", (data: { conversationId: string, receiverIds: string[], name?: string }) => {
                if (!data?.conversationId || !data?.receiverIds || !Array.isArray(data.receiverIds)) return;
                this.emitToUsers(data.receiverIds, "typing", {
                    conversationId: data.conversationId,
                    userId,
                    name: data.name || "Ai đó"
                })
            });

            socket.on("stop_typing", (data: { conversationId: string, receiverIds: string[] }) => {
                if (!data?.conversationId || !data?.receiverIds || !Array.isArray(data.receiverIds)) return;
                
                this.emitToUsers(data.receiverIds, "stop_typing", {
                    conversationId: data.conversationId,
                    userId
                })
            });

            // Watermark Handlers
            socket.on("mark_delivered", async (data: { conversationId: string, messageId: string }) => {
                if (!data?.conversationId || !data?.messageId) return;
                await this.handleWatermarkUpdate(userId, data.conversationId, data.messageId, 'delivered');
            });

            socket.on("mark_read", async (data: { conversationId: string, messageId: string }) => {
                if (!data?.conversationId || !data?.messageId) return;
                await this.handleWatermarkUpdate(userId, data.conversationId, data.messageId, 'read');
            });

            // Disconnect handler
            this.registerDisconnectHandler(socket, userId);
        });
    }

    private async handleWatermarkUpdate(userId: string, conversationId: string, messageId: string, type: 'delivered' | 'read') {
        try {
            // Update Redis
            const key = `watermarks:${conversationId}`;
            const field = userId;
            
            let currentStr = await redisClient.getClient().hget(key, field);
            let current = currentStr ? JSON.parse(currentStr) : {};
            
            if (type === 'delivered') {
                current.last_delivered_msg_id = messageId;
            } else {
                current.last_read_msg_id = messageId;
            }
            
            await redisClient.getClient().hset(key, field, JSON.stringify(current));

            // Publish to RabbitMQ
            const { syncWatermarkToDB } = await import('#@/infrastructure/rabbitmq/producer.js');
            syncWatermarkToDB({ conversationId, userId, messageId, type });

            // Lấy danh sách thành viên để emit socket
            const { conversationFacade } = await import('#@/modules/conversation/conversation.facade.js');
            const members = await conversationFacade.getConversationMembers(conversationId, userId);
            
            // Emit to group
            this.emitToUsers(members, "watermark_updated", {
                conversationId,
                userId,
                messageId,
                type
            });
        } catch (error) {
            console.error(`[Socket.IO] Error handling watermark update:`, error);
        }
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

            // Debounce for 3 seconds to handle page refresh / multiple tabs
            setTimeout(async () => {
                try {
                    const isOnline = await this.isUserOnline(userId);
                    if (!isOnline) {
                        const lastActive = new Date();
                        
                        // Update presence in Redis to offline
                        await redisClient.setJSON(`presence:${userId}`, { 
                            status: 'offline', 
                            last_active: lastActive 
                        }, 24 * 3600);
    
                        // Update database
                        await userContainer.userService.updatePresence(userId, lastActive);
    
                        // Broadcast offline event to all other users
                        this.io.to(ALL_USERS_ROOM).emit("user_offline", { 
                            userId, 
                            last_active: lastActive 
                        });
                    }
                } catch (error) {
                    console.error("[Socket.IO] Error handling disconnect presence:", error);
                }
            }, 3000);
        });
    }


    public emitToUser(
        userId: string,
        event: string,
        data: unknown,
    ): void {
        this.io
            .to(USER_ROOM(userId))
            .emit(event, data);
    }


    public joinGroup(
        userId: string,
        conversationId: string,
    ): void {
        const room = CONVERSATION_ROOM(conversationId);

        this.io
            .in(USER_ROOM(userId))
            .socketsJoin(room);

        console.log(
            `[Socket.IO] User '${userId}' joined conversation '${conversationId}'`
        );
    }

    public leaveGroup(
        userId: string,
        conversationId: string,
    ): void {
        const room = CONVERSATION_ROOM(conversationId);

        this.io
            .in(USER_ROOM(userId))
            .socketsLeave(room);

        console.log(
            `[Socket.IO] User '${userId}' left conversation '${conversationId}'`
        );
    }

    public emitToGroup(
        conversationId: string,
        event: string,
        data: unknown,
    ): void {
        const room = CONVERSATION_ROOM(conversationId);

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
    server: HttpServer
): SocketManager => {
    socketManager = new SocketManager(
        server
    );

    return socketManager;
};