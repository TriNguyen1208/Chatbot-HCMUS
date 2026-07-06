import type {Request, Response, NextFunction, RequestHandler } from "express";
import { z, ZodError } from "zod";
import createHttpError from "http-errors";

export const validate = (schema: z.ZodType<any, any, any>): RequestHandler => {
    return async (req: Request, res: Response, next: NextFunction): Promise<void> => {
        try{            
            const parsed = await schema.parseAsync({
                body: req.body,
                query: req.query,
                params: req.params,
            });
            if (parsed.body) req.body = parsed.body;
            if (parsed.query) req.query = parsed.query;
            if (parsed.params) req.params = parsed.params;
            return next()
        }catch(error){
            if (error instanceof ZodError) {
                const message = error.message;
                return next(createHttpError.BadRequest(message));
            }
            return next(error);
        }
    }
}
