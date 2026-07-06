import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";

const PUBLIC_ROUTES = ["/"];
const PROTECTED_PREFIXES = ["/home", "/messages", "/profile", "/feed"];

export function middleware(request: NextRequest) {
  const { pathname } = request.nextUrl;
  const accessToken = request.cookies.get("accessToken")?.value;
  const refreshToken = request.cookies.get("refreshToken")?.value;

  const isProtected = PROTECTED_PREFIXES.some((p) => pathname.startsWith(p));
  const isPublic = PUBLIC_ROUTES.includes(pathname);

  // Redirect unauthenticated users away from protected routes
  if (isProtected && !accessToken && !refreshToken) {
    return NextResponse.redirect(new URL("/", request.url));
  }

  // Redirect already-authenticated users away from login
  if (isPublic && (accessToken || refreshToken)) {
    return NextResponse.redirect(new URL("/home", request.url));
  }

  return NextResponse.next();
}

export const config = {
  matcher: ["/((?!_next/static|_next/image|favicon.ico|api/).*)"],
};
