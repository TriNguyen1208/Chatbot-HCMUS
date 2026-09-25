import client from "prom-client";
import type { Request, Response, NextFunction } from "express";

// 1. Tự động kích hoạt bộ thu thập metrics mặc định của Node.js:
// Đo Event Loop lag, Heap memory (RAM), CPU usage, Garbage Collection...
client.collectDefaultMetrics({ prefix: "chatbot_" });

// 2. Custom Metric: Đo số lượng kết nối Socket.IO đang online (Gauge: có thể tăng/giảm)
export const socketActiveConnections = new client.Gauge({
    name: "chatbot_socket_active_connections",
    help: "Số lượng kết nối Socket.IO đang hoạt động đồng thời",
});

// 3. Custom Metric: Tổng số lượng HTTP request đã tiếp nhận (Counter: chỉ tăng)
export const httpRequestsTotal = new client.Counter({
    name: "chatbot_http_requests_total",
    help: "Tổng số lượng HTTP requests",
    labelNames: ["method", "route", "status"],
});

// 4. Custom Metric: Thời gian xử lý HTTP request (Histogram: đo latency p50, p95, p99)
export const httpRequestDurationSeconds = new client.Histogram({
    name: "chatbot_http_request_duration_seconds",
    help: "Thời gian xử lý HTTP requests (giây)",
    labelNames: ["method", "route", "status"],
    buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
});

// 5. Middleware đo lường HTTP Requests cho Express
export const metricsMiddleware = (req: Request, res: Response, next: NextFunction): void => {
    // Bỏ qua chính endpoint /metrics để không tự làm loãng dữ liệu đo
    if (req.path === "/metrics") {
        return next();
    }

    const start = process.hrtime();

    res.on("finish", () => {
        const diff = process.hrtime(start);
        const durationInSeconds = diff[0] + diff[1] / 1e9;

        // Chuẩn hóa tên route để tránh phân mảnh nhãn (high-cardinality)
        const route = req.route?.path ? `${req.baseUrl || ""}${req.route.path}` : req.baseUrl || req.path;
        const status = res.statusCode.toString();

        httpRequestsTotal.inc({
            method: req.method,
            route,
            status,
        });

        httpRequestDurationSeconds.observe(
            {
                method: req.method,
                route,
                status,
            },
            durationInSeconds
        );
    });

    next();
};

// Xuất registry chứa toàn bộ metrics
export const register = client.register;
