1. Suy nghĩ và đánh giá có nên dùng cluster trong nodejs để có nhiều main thread tận dụng tối đa cpu hay không
+ Hiện tại là bullMQ đang chạy chung luồng main thread, nếu như mà không tách luồng thì nó chặn
+ Vậy chắc chắn là phải thêm 1 luồng thread chính chỉ để chạy worker ngầm

Vấn đề về xung đột khi chia nhiều thread:
+ Nếu như nhiều thread thì có thể socket này nằm ở 1 thread còn socket kia nằm ở thread khác
+ Phải có 1 cái chung gọi là socket.io/adaper, khi có 1 sự kiện đi tới thì đi vào redis trước, sau đó mới broadcast tới socket
+ Tức là Socket.IO không tự handle được, nó phải dùng Redis Adapter để handle khi có nhiều process
+ Tức là nó sẽ tốn tài nguyên hơn, tuy nhiên nó sẽ tận dụng được cpu
+ Vấn đề về race condition (đánh giá xem có cần phải mutex không)
+ Phải sticky session (do là để request không đổi sang socket khác)

Thêm nữa, thằng nào là thằng điều phối các worker trong cluster mode (khi chia thread)
