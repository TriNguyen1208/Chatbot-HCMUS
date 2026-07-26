import type { Response } from "express";
import { parseDurationMs } from "./time.utils.js";
import { config } from "../config/config.js";

export const setCookie = (
    res: Response,
    name_token: string,
    token: string,
    timeExpire: number,
    path?: string
): void => {
    res.cookie(name_token, token, {
        httpOnly: true,
        secure: true,
        sameSite: "strict",
        path: path,
        maxAge: timeExpire
    })
}

export const clearCookie = (
    res: Response,
    name_token: string,
    path?: string
) => {
    res.clearCookie(name_token, {
        httpOnly: true,
        secure: true,
        sameSite: "strict",
        path: path,
    });
}