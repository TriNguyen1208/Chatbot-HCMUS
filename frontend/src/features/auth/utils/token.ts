"use client";

import Cookies from "js-cookie";
import { constant } from "@/config/constant";
import { TokenPair } from "@/features/auth/index";

export function setTokens(tokens: TokenPair) {
    Cookies.set("accessToken", tokens.accessToken, {
        secure: true,
        sameSite: "strict",
        expires: constant.accessExpires,
    });
    if (tokens.refreshToken) {
        Cookies.set("refreshToken", tokens.refreshToken, {
            secure: true,
            sameSite: "strict",
            expires: constant.refreshExpires,
        });
    }
}
export function clearTokens() {
    Cookies.remove("accessToken");
    Cookies.remove("refreshToken");
}