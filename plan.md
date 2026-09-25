1. Trước mắt là chưa handle việc race condition

2. Hỗ trợ retry khi mạng lỗi: 
+ Retry khi mà gặp vấn đề quá tải, timeout,..
+ Dùng Exponential Backoff với Jitter để tránh việc gửi yêu cầu quá nhiều cùng lúc
+ 1 khi mà đã retry thì phải idempotency (không được phép tạo nhóm 2 lần, hoặc là gửi tin nhắn 2 lần)
+ Phải đặt retry count, timeout
+ Circuit Breaker Pattern: Tức là khi request thất bại quá 1 ngưỡng (ví dụ như 50% trong 10 giây), thì fail-fast không cho retry nữa
+ idempotency dùng để bảo vệ các tầng:
    - User bấm gửi 2 lần liên tiếp (do ứng dụng hoặc mạng chậm)
    - Mạng rớt nên retry
    - worker xử lý nền

3. Redis để rate limit từng services
4. Logging and Tracing and Monitoring hệ thống
+ Dùng grafana, prometheus, loki và jeager
5. Sau đó thử testing xem hệ thống có thể hỗ trợ được bao nhiêu user
