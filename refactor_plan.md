Giai đoạn 1:
- Để giảm bớt độ phức tạp của vấn đề: không dùng supabase nữa mà chỉ dùng mongoDB thôi (không cần phải dùng supabase nữa) (dùng trực tiếp populate của mongoDB thay vì phải populateUser database khác, tốn query)
- Luôn luôn trả về cho frontend với dữ liệu đầy đủ (tức là không chỉ có id và phải là user,...)



Giai đoạn 2: 
- Hướng làm online/offline: Nếu người dùng kết nối socket, dùng redis để lưu trạng thái người đó online, khi đó ta sẽ emitToUser tất cả bạn bè (conversation 'utu' của người đó với các người khác) (không limit bằng 100), frontend sẽ lắng nghe và thay đổi trạng thái thàng online. Tương tự như vậy, gửi thông báo khi user offline, lưu vào redis là offline (để frontend thay đổi trạng thái).
- Việc lưu last_active thì vẫn là dùng redis để lưu thời gian gần nhất người đó online sau đó dùng background worker cập nhật ngầm (bullMQ) vào database




