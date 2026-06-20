import type { RequestHandler } from "express"

const asyncHandler = (fn: RequestHandler): RequestHandler => async (req, res, next) => {
    try {
        await fn(req, res, next);
    } catch (error) {
        next(error);
    }
};

export default asyncHandler;