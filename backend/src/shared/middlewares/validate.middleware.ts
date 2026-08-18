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
            if (parsed.body) {
                Object.defineProperty(req, 'body', { value: parsed.body, writable: true, configurable: true });
            }
            if (parsed.query) {
                Object.defineProperty(req, 'query', { value: parsed.query, writable: true, configurable: true });
            }
            if (parsed.params) {
                Object.defineProperty(req, 'params', { value: parsed.params, writable: true, configurable: true });
            }
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
