import { z } from "zod";
import type { SocketAckResponse } from "./socket.types.js";

/**
 * Validates incoming socket event payloads against a Zod schema.
 * If validation fails, automatically invokes the acknowledgement callback (if provided)
 * with a standardized 400 Bad Request error response and returns null.
 * If validation succeeds, returns the strongly-typed data.
 */
export const validateSocketPayload = <T>(
    schema: z.ZodType<T>,
    data: unknown,
    ack?: (res: SocketAckResponse) => void
): T | null => {
    const result = schema.safeParse(data);
    if (!result.success) {
        const firstIssue = result.error.issues[0];
        const errorMessage = firstIssue
            ? (firstIssue.path.length > 0 ? `${firstIssue.path.join('.')}: ${firstIssue.message}` : firstIssue.message)
            : "Dữ liệu không hợp lệ";

        ack?.({
            success: false,
            code: 400,
            message: errorMessage
        });
        return null;
    }
    return result.data;
};
