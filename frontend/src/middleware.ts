import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";

export function middleware(req: NextRequest) {
  const token = req.cookies.get("accessToken")?.value || req.cookies.get("refreshToken")?.value;
  const { pathname } = req.nextUrl;

  const isAuthRoute = pathname === "/";
  const isProtectedRoute =
    pathname.startsWith("/chat") ||
    pathname.startsWith("/direct-chat") ||
    pathname.startsWith("/group-chat") ||
    pathname.startsWith("/home") ||
    pathname.startsWith("/profile");

  // 1. Chưa đăng nhập mà truy cập route được bảo vệ -> Redirect về trang Login
  if (!token && isProtectedRoute) {
    const loginUrl = new URL("/", req.url);
    return NextResponse.redirect(loginUrl);
  }

  // 2. Đã đăng nhập mà truy cập trang Login (/) -> Redirect vào /chat
  if (token && isAuthRoute) {
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
