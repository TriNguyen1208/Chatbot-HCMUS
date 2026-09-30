import pino from "pino";
import util from "node:util";
import { trace } from "@opentelemetry/api";

// 1. Khởi tạo thực thể Pino Logger siêu tốc độ (Non-blocking JSON logger)
export const logger = pino({
    level: process.env.LOG_LEVEL || "info",
    timestamp: pino.stdTimeFunctions.isoTime,
    base: {
        instance_port: process.env.PORT || "5000",
    },
});

/**
 * 2. Tự động chuyển hướng toàn bộ console.log, console.error, console.warn sang Pino
 * và tiêm trace_id của OpenTelemetry vào JSON nếu đang ở trong một request.
 */
export const initLogging = (): void => {
    console.log = (...args: unknown[]): void => {
        const activeSpan = trace.getActiveSpan();
        const spanContext = activeSpan?.spanContext();
        const msg = util.format(...args);

        if (spanContext?.traceId) {
            logger.info({ trace_id: spanContext.traceId, span_id: spanContext.spanId }, msg);
        } else {
            logger.info(msg);
        }
    };

    console.info = console.log;

    console.error = (...args: unknown[]): void => {
        const activeSpan = trace.getActiveSpan();
        const spanContext = activeSpan?.spanContext();
        const msg = util.format(...args);

        if (spanContext?.traceId) {
            logger.error({ trace_id: spanContext.traceId, span_id: spanContext.spanId }, msg);
        } else {
            logger.error(msg);
        }
    };

    console.warn = (...args: unknown[]): void => {
        const activeSpan = trace.getActiveSpan();
        const spanContext = activeSpan?.spanContext();
        const msg = util.format(...args);

        if (spanContext?.traceId) {
            logger.warn({ trace_id: spanContext.traceId, span_id: spanContext.spanId }, msg);
        } else {
            logger.warn(msg);
        }
    };

    console.debug = (...args: unknown[]): void => {
        const activeSpan = trace.getActiveSpan();
        const spanContext = activeSpan?.spanContext();
        const msg = util.format(...args);

        if (spanContext?.traceId) {
            logger.debug({ trace_id: spanContext.traceId, span_id: spanContext.spanId }, msg);
        } else {
            logger.debug(msg);
        }
    };
};

// Tự động kích hoạt ngay khi file được nạp
initLogging();
