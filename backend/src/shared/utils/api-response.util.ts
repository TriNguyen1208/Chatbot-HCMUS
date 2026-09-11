import type { Request, Response, NextFunction } from "express";

export class APIResponse{
    success<T>(
        res: Response, 
        data?: T, 
        options?: {
            statusCode?: number;
            message?: string;
            meta?: any;
        }
    ): Response{
        return res.status(options?.statusCode ?? 200).json({
            data: data,
            message: options?.message,
            meta: options?.meta,
        })
    }
}

export const apiResponse = new APIResponse()