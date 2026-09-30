import { NodeSDK } from "@opentelemetry/sdk-node";
import { getNodeAutoInstrumentations } from "@opentelemetry/auto-instrumentations-node";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";

// Mặc định bật Tracing nếu không đặt rõ là 'false'
const isTracingEnabled = process.env.ENABLE_TRACING !== "false";

if (isTracingEnabled) {
    //Giúp phân biệt được các instance với nhau, request này thuộc port nào
    const instancePort = process.env.PORT || "5000";
    const instanceId = process.env.INSTANCE_ID || `backend-port-${instancePort}`;
    if (!process.env.OTEL_RESOURCE_ATTRIBUTES) {
        process.env.OTEL_RESOURCE_ATTRIBUTES = `service.instance.id=${instanceId}`;
    }

    const traceExporter = new OTLPTraceExporter({
        url: process.env.OTEL_EXPORTER_OTLP_ENDPOINT || "http://localhost:4318/v1/traces",
    });

    const sdk = new NodeSDK({
        serviceName: "chatbot-backend",
        traceExporter,
        instrumentations: [
            getNodeAutoInstrumentations({
                // Tắt instrumentation file system (fs) để tránh spam hàng ngàn span đọc file tĩnh
                "@opentelemetry/instrumentation-fs": {
                    enabled: false,
                },
            }),
        ],
    });

    sdk.start();
    console.log("🔭 OpenTelemetry Tracing đã kích hoạt (xuất sang Jaeger port 4318)");

    process.on("SIGTERM", () => {
        sdk.shutdown().catch((err) => console.error("Lỗi đóng OpenTelemetry:", err));
    });
}
