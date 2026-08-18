import { parseCookie } from "cookie";
import { jwtService } from "#@/shared/utils/jwt-services.js";

export const socketAuthMiddleware = (socket: any, next: any) => {
    const cookies = parseCookie(socket.handshake.headers.cookie || "");
    const token = cookies.accessToken; 
    
    if (!token) {
        return next(new Error("Authentication error: Missing access token"));
    }

    const user = jwtService.verifyAccessToken(token);
    if (!user || !user.userID) {
        return next(new Error("Authentication error: Invalid token"));
    }

    socket.data.userId = user.userID; 
    next();
}