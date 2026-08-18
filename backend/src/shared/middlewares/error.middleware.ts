import type { Request, Response, NextFunction } from "express";

export interface AppError extends Error {
    statusCode?: number,
    code?: string
}

export const errorHandler = (
    err: AppError,
    _req: Request,
    res: Response,
    next: NextFunction
): void => {
    if (!err.statusCode || err.statusCode === 500) {
        console.error("Bắt được lỗi 500:", err);
    }
    const statusCode = err.statusCode || 500
    const code = err.code || "Internal_Error"

    res.status(statusCode).json({
        code,
        message: statusCode === 500 ? "Internal Server Error" : err.message
    })
}
