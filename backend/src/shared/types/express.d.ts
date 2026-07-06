import type { JWTPayload } from "#@/types/index.ts";

declare global {
    namespace Express {
        interface Request {
            user?: JWTPayload;
        }
    }
}

export {};