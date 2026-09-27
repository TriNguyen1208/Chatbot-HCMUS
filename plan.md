1. Trước mắt là chưa handle việc race condition
5. Sau đó thử testing xem hệ thống có thể hỗ trợ được bao nhiêu user
6. Chạy folder backend ở 2 port khác nhau. Sau đó dùng nginx để làm load balancer điều phối request vào 2 port này. Nếu như 1 port chết thì toàn bộ request vào 1 port. Lưu ý: việc tracing, monitoring, logging vẫn hoạt động bình thường