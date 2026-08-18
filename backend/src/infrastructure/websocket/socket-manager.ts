// import { Server as HttpServer } from "http";
// import { Server, Socket } from "socket.io";
// import { socketAuthMiddleware } from "#@/shared/middlewares/socket-auth.middleware.js";
// import { config } from "#@/config/config.js";

// // Lớp quản lý toàn bộ các thao tác liên quan đến WebSocket
// export class SocketManager {
//     private io: Server;

//     // Map dùng để ánh xạ giữa userId và danh sách các socketId (hỗ trợ 1 user mở nhiều tab)
//     private userSocketMap = new Map<string, string[]>();

//     constructor(server: HttpServer) {
//         // Khởi tạo server Socket.IO
//         this.io = new Server(server, {
//             cors: {
//                 origin: config.corsOrigins, // Cấu hình domain được phép kết nối
//                 credentials: true // Cho phép gửi cookie/thông tin xác thực
//             }
//         });

//         // Sử dụng middleware để xác thực kết nối socket
//         this.io.use(socketAuthMiddleware);

//         // Lắng nghe sự kiện khi có một client kết nối tới
//         this.io.on("connection", async (socket: Socket) => {
//             // Lấy userId đã được đính kèm từ middleware xác thực
//             const userId = socket.data.userId;
            
//             if (userId) {
//                 // Hỗ trợ nhiều kết nối (nhiều tab) cho cùng một user
//                 // Lấy danh sách socketId hiện tại của user, nếu chưa có thì khởi tạo mảng rỗng
//                 let userSockets = this.userSocketMap.get(userId) || [];
                
//                 // Thêm socketId mới vào mảng nếu nó chưa tồn tại
//                 if (!userSockets.includes(socket.id)) {
//                     userSockets.push(socket.id);
//                 }
                
//                 // Cập nhật lại danh sách socket vào Map
//                 this.userSocketMap.set(userId, userSockets);

//                 try {
//                     // Import động container để lấy ra service xử lý conversation (tránh bị vòng lặp import)
//                     const { conversationContainer } = await import("#@/modules/conversation/conversation.container.js");
                    
//                     // Truy vấn DB để lấy ra danh sách các cuộc trò chuyện (tối đa 100) của user này
//                     const conversations = await conversationContainer.conversationService.getConversationList(userId, 100);
                    
//                     // Sửa lỗi Race Condition (Xung đột thời gian thực)
//                     // Kiểm tra xem sau khi DB query xong, socket này có còn nằm trong Map của user hay không.
//                     // Việc này đề phòng trường hợp socket đã bị ngắt kết nối (do f5, tắt tab) trong lúc đang chờ truy vấn DB
//                     if (!this.userSocketMap.get(userId)?.includes(socket.id)) {
//                         console.log(`[Socket.IO] Socket ${socket.id} đã bị ngắt kết nối trong lúc truy vấn DB, huỷ bỏ thao tác join room`);
//                         return;
//                     }

//                     // Duyệt qua tất cả cuộc trò chuyện và cho socket join vào room (groupId) tương ứng
//                     conversations.forEach(c => {
//                         if (c._id || (c as any).id) {
//                             const groupId = (c._id || (c as any).id)!.toString();
                            
//                             // Nếu socket chưa nằm trong room thì tiến hành join
//                             if (!socket.rooms.has(groupId)) {
//                                 console.log(`[Socket.IO] User '${userId}' (socket: ${socket.id}) join vào nhóm '${groupId}'`);
//                                 socket.join(groupId);
//                             }
//                         }
//                     });
//                 } catch (err) {
//                     console.error("Lỗi khi tham gia các rooms lúc khởi tạo kết nối", err);
//                 }
//             }

//             // Lắng nghe sự kiện ngắt kết nối (tắt tab, f5, mất mạng...)
//             socket.on("disconnect", (reason) => {
//                 console.log(`[Socket.IO] Socket ${socket.id} đã ngắt kết nối. Lý do: ${reason}`);
                
//                 if (userId) {
//                     // Lấy ra danh sách các socket hiện tại của user
//                     let userSockets = this.userSocketMap.get(userId);
//                     if (userSockets) {
//                         // Lọc bỏ socket.id vừa bị ngắt kết nối ra khỏi mảng
//                         userSockets = userSockets.filter(id => id !== socket.id);
                        
//                         // Nếu user không còn socket nào (đã tắt hết các tab), thì xoá user khỏi Map hoàn toàn
//                         if (userSockets.length === 0) {
//                             console.log(`[Socket.IO] User '${userId}' đã bị xoá hoàn toàn khỏi map`);
//                             this.userSocketMap.delete(userId);
//                         } else {
//                             // Cập nhật lại mảng socketIds cho user
//                             this.userSocketMap.set(userId, userSockets);
//                         }
//                     }
//                 }
//             });
//         });
//     }

//     // Gửi sự kiện trực tiếp đến một user cụ thể (gửi đến tất cả các tab của họ)
//     public emitToUser(userId: string, event: string, data: any) {
//         const socketIds = this.userSocketMap.get(userId);

//         if (socketIds && socketIds.length > 0) {
//             // Duyệt qua tất cả các socket (tab) của user và gửi tín hiệu
//             socketIds.forEach(socketId => {
//                 this.io.to(socketId).emit(event, data);
//             });
//         }
//     }

//     // Cho một user join vào một nhóm trò chuyện (join trên tất cả các tab của họ)
//     public joinGroup(userId: string, groupId: string) {
//         const socketIds = this.userSocketMap.get(userId);
//         if (socketIds && socketIds.length > 0) {
//             socketIds.forEach(socketId => {
//                 // Lấy ra đối tượng socket thực tế từ socketId
//                 const socket = this.io.sockets.sockets.get(socketId);
                
//                 // Kiểm tra và tiến hành join nếu chưa có trong room
//                 if (socket && !socket.rooms.has(groupId)) {
//                     console.log(`[Socket.IO] User '${userId}' join vào nhóm '${groupId}'`);
//                     socket.join(groupId);
//                 }
//             });
//         }
//     }

//     // Cho một user rời khỏi một nhóm trò chuyện (rời khỏi trên tất cả các tab)
//     public leaveGroup(userId: string, groupId: string) {
//         const socketIds = this.userSocketMap.get(userId);
//         if (socketIds && socketIds.length > 0) {
//             socketIds.forEach(socketId => {
//                 const socket = this.io.sockets.sockets.get(socketId);
//                 if (socket) {
//                     socket.leave(groupId);
//                 }
//             });
//         }
//     }

//     // Gửi tín hiệu đến tất cả các thành viên trong một room cụ thể
//     public emitToGroup(groupId: string, event: string, data: any) {
//         const room = this.io.sockets.adapter.rooms.get(groupId);

//         console.log(`[Socket.IO] Đang phát tín hiệu ${event}`, {
//             groupId,
//             socketIds: room ? [...room] : [],
//             roomSize: room?.size ?? 0, // In ra số lượng socket đang ở trong room
//         });
//         console.log(`[Socket.IO] Phát sóng (Broadcast) sự kiện '${event}' tới room '${groupId}'`);
        
//         // Phát sự kiện tới nhóm
//         this.io.to(groupId).emit(event, data);
//     }

//     // Gửi tín hiệu đến danh sách nhiều user (ví dụ gửi tin nhắn đồng loạt)
//     public emitToUsers(userIds: string[], event: string, data: any) {
//         userIds.forEach(userId => {
//             console.log("Gửi tín hiệu đến user: ", userId, data)
//             this.emitToUser(userId, event, data); // Tận dụng hàm gửi cho 1 user
//         });
//     }

//     // Ép ngắt kết nối một user (ngắt trên tất cả các tab)
//     public disconnectUser(userId: string) {
//         const socketIds = this.userSocketMap.get(userId);
//         if (socketIds && socketIds.length > 0) {
//             socketIds.forEach(socketId => {
//                 this.io.in(socketId).disconnectSockets(true);
//             });
//         }
//     }
    
//     // Lấy ra toàn bộ danh sách socket đang được ánh xạ (dùng cho debug/quản trị)
//     public getUserSocketMap() {
//         return this.userSocketMap;
//     }
// }

// export let socketManager: SocketManager;

// // Khởi tạo SocketManager và gán vào biến toàn cục
// export const initSocket = (server: HttpServer) => {
//     socketManager = new SocketManager(server);
//     return socketManager;
// }
import { Server as HttpServer } from "http";
import { Server, Socket } from "socket.io";

import { socketAuthMiddleware } from "#@/shared/middlewares/socket-auth.middleware.js";
import { config } from "#@/config/config.js";

// 🔥 CHANGED:
// Import type/service trực tiếp thay vì dynamic import container
import type { ConversationService } from "#@/modules/conversation/services/conversation.service.js";


// 🔥 CHANGED:
// Centralize room naming.
// Tránh dùng trực tiếp groupId vì sau này có thể có nhiều loại room.
const USER_ROOM = (userId: string) => `user:${userId}`;

const CONVERSATION_ROOM = (conversationId: string) =>
    `conversation:${conversationId}`;


export class SocketManager {
    private readonly io: Server;

    // 🔥 CHANGED:
    // ConversationService được inject từ bên ngoài.
    // Không còn dynamic import để tránh circular dependency.
    constructor(
        server: HttpServer,
        private readonly conversationService: ConversationService,
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
            // 🔥 CHANGED:
            // userId đã được xác thực bởi socketAuthMiddleware.
            const userId = socket.data.userId as string | undefined;

            if (!userId) {
                console.warn(
                    `[Socket.IO] Socket ${socket.id} connected without userId`
                );

                socket.disconnect(true);
                return;
            }

            // 🔥 CHANGED:
            // Không cần userSocketMap nữa.
            //
            // Tất cả socket của cùng user sẽ join:
            //
            // user:123
            //
            // Vì vậy multi-tab / multi-device tự động được hỗ trợ.
            socket.join(USER_ROOM(userId));

            console.log(
                `[Socket.IO] User '${userId}' connected with socket '${socket.id}'`
            );

            // Load conversation rooms asynchronously
            void this.joinUserConversationRooms(socket, userId);

            // Disconnect handler
            this.registerDisconnectHandler(socket, userId);
        });
    }


    /**
     * Join socket vào tất cả conversation rooms
     * mà user hiện tại là thành viên.
     */
    private async joinUserConversationRooms(
        socket: Socket,
        userId: string,
    ): Promise<void> {
        try {
            const conversations =
                await this.conversationService.getConversationList(
                    userId,
                    100,
                );

            // 🔥 CHANGED:
            // Race-condition protection.
            //
            // Nếu user F5 / đóng tab trong lúc đang query DB,
            // socket.connected sẽ trở thành false.
            if (!socket.connected) {
                console.log(
                    `[Socket.IO] Socket '${socket.id}' disconnected while loading conversations`
                );

                return;
            }

            for (const conversation of conversations) {
                const conversationId =
                    conversation._id?.toString() ??
                    (conversation as any).id?.toString();

                if (!conversationId) {
                    continue;
                }

                const room = CONVERSATION_ROOM(conversationId);

                // 🔥 CHANGED:
                // Socket.IO tự quản lý socket.rooms.
                if (!socket.rooms.has(room)) {
                    socket.join(room);

                    console.log(
                        `[Socket.IO] Socket '${socket.id}' joined '${room}'`
                    );
                }
            }
        } catch (error) {
            console.error(
                `[Socket.IO] Failed to join conversation rooms for user '${userId}'`,
                error,
            );
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

            // 🔥 CHANGED:
            //
            // Không cần remove socket khỏi Map.
            //
            // Socket.IO tự động remove socket khỏi:
            //
            // user:${userId}
            // conversation:${conversationId}
            //
            // khi disconnect.
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


    /**
     * Force disconnect all active sockets of a user.
     */
    public disconnectUser(userId: string): void {
        // 🔥 CHANGED:
        //
        // user:${userId} chứa tất cả socket của user.
        //
        // Không cần Map<userId, socketIds>.
        this.io
            .in(USER_ROOM(userId))
            .disconnectSockets(true);

        console.log(
            `[Socket.IO] Force disconnected user '${userId}'`
        );
    }


    /**
     * Check whether user currently has at least one connection.
     */
    public async isUserOnline(userId: string): Promise<boolean> {
        // 🔥 CHANGED:
        // Không cần expose internal Map.
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
): SocketManager => {
    socketManager = new SocketManager(
        server,
        conversationService,
    );

    return socketManager;
};