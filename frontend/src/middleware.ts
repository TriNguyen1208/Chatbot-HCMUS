import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";

export function middleware(req: NextRequest) {
    const accessToken = req.cookies.get("accessToken")?.value;
    const refreshToken = req.cookies.get("refreshToken")?.value;
    const { pathname } = req.nextUrl;

    const isAuthRoute = pathname === "/";
    const isProtectedRoute =
        pathname.startsWith("/chat") ||
        pathname.startsWith("/direct-chat") ||
        pathname.startsWith("/group-chat") ||
        pathname.startsWith("/profile");

    // 1. Không có bất kỳ token nào mà vào trang được bảo vệ -> Redirect về trang Login
    if (!accessToken && !refreshToken && isProtectedRoute) {
        const loginUrl = new URL("/", req.url);
        return NextResponse.redirect(loginUrl);
    }
    // 2. Chỉ chuyển hướng từ trang Login vào /chat khi ĐÃ CÓ accessToken hợp lệ.
    if ((accessToken || refreshToken) && isAuthRoute) {
        const chatUrl = new URL("/chat", req.url);
        return NextResponse.redirect(chatUrl);
    }

    return NextResponse.next();
}

export const config = {
    matcher: [
        /*
         * Match all request paths except:
         * - _next/static (static files)
         * - _next/image (image optimization files)
         * - favicon.ico (favicon file)
         * - public files (images, assets)
         * - api routes (handled directly or proxied)
         */
        "/((?!api|_next/static|_next/image|favicon.ico|.*\\.(?:svg|png|jpg|jpeg|gif|webp)$).*)",
    ],
};
