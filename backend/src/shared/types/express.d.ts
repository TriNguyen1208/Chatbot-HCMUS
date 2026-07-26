import type { JWTPayload } from "#@/shared/types/index.ts";

declare global {
    namespace Express {
        interface Request {
            user?: JWTPayload;
        }
    }
}

export {};