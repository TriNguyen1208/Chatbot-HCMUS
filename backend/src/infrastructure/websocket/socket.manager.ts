import { Server as HttpServer } from "http";
import { Server, Socket } from "socket.io";
import { createAdapter } from "@socket.io/redis-adapter";
import { socketAuthMiddleware } from "./socket-auth.middleware.js";
import { config } from "#@/config/config.js";
import { redisClient } from "#@/infrastructure/redis/redis.client.js";
import { registerSocketHandlers } from "./handlers/index.js";

export const USER_ROOM = (userId: string) => `user:${userId}`;
export const ALL_USERS_ROOM = 'global:all_users';
export const CONVERSATION_ROOM = (conversationId: string) => `conversation:${conversationId}`;

export class SocketManager {
    private readonly io: Server;

    constructor(server: HttpServer) {
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
                console.warn(`[Socket.IO] Socket ${socket.id} connected without userId`);
                socket.disconnect(true);
                return;
            }

            socket.join(USER_ROOM(userId));
            socket.join(ALL_USERS_ROOM);

            console.log(`[Socket.IO] User '${userId}' connected with socket '${socket.id}'`);

            // Register modular event handlers (presence, typing, watermark)
            registerSocketHandlers(this.io, socket, this, ALL_USERS_ROOM);
        });
    }

    public emitToUser(userId: string, event: string, data: unknown): void {
        this.io.to(USER_ROOM(userId)).emit(event, data);
    }

    public emitToUsers(userIds: string | string[], event: string, data: unknown): void {
        const uids = Array.isArray(userIds) ? userIds : [userIds];
        if (uids.length === 0) return;
        const userRooms = uids.map(id => USER_ROOM(id));
        this.io.to(userRooms).emit(event, data);
    }

    public joinGroup(userIds: string | string[], conversationId: string): void {
        const uids = Array.isArray(userIds) ? userIds : [userIds];
        if (uids.length === 0) return;
        const room = CONVERSATION_ROOM(conversationId);
        const userRooms = uids.map(id => USER_ROOM(id));
        console.log(userRooms)
        console.log(room)
        this.io.in(userRooms).socketsJoin(room);
        console.log(`[Socket.IO] Users [${uids.join(", ")}] joined conversation '${conversationId}'`);
    }

    public leaveGroup(userIds: string | string[], conversationId: string): void {
        const uids = Array.isArray(userIds) ? userIds : [userIds];
        if (uids.length === 0) return;
        const room = CONVERSATION_ROOM(conversationId);
        const userRooms = uids.map(id => USER_ROOM(id));
        this.io.in(userRooms).socketsLeave(room);
        console.log(`[Socket.IO] Users [${uids.join(", ")}] left conversation '${conversationId}'`);
    }

    public emitToGroup(conversationId: string, event: string, data: unknown): void {
        const room = CONVERSATION_ROOM(conversationId);
        this.io.to(room).emit(event, data);
    }

    public disconnectUser(userId: string): void {
        this.io.in(USER_ROOM(userId)).disconnectSockets(true);
        console.log(`[Socket.IO] Force disconnected user '${userId}'`);
    }

    public async isUserOnline(userId: string): Promise<boolean> {
        const sockets = await this.io.in(USER_ROOM(userId)).fetchSockets();
        return sockets.length > 0;
    }

    public getIO(): Server {
        return this.io;
    }
}

export let socketManager: SocketManager;

export const initSocket = (server: HttpServer): SocketManager => {
    socketManager = new SocketManager(server);
    return socketManager;
};
