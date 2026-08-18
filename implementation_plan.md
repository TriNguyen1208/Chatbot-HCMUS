1. Tận dụng sức mạnh của redis, khi mà ghi vào database thì xoá cache trong redis, khi nào người dùng lần đầu get redis thì redis cache miss, sau đó redis mới tận dụng lấy dữ liệu từ database nạp vào redis
2. Chuyển google đăng nhập thành outlook đăng nhập (để dễ lấy mssv và tên hơn)
3. Chuyển tin nhắn từ client sang server (dùng socket luôn thay vì là phương thức HTTP)
4. Gửi icon, emoji, hỗ trợ react