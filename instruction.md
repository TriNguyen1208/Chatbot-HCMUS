5. Hiện tại lỗi delivered, read,...
7. Refactor lại toàn bộ cấu trúc frontend và backend (Phải tối ưu)
8. Thêm AI Chat vào cả trò chuyện riêng với AI và tích hợp AI vào cuộc hội thoại
9. Sửa SEO cho trang web (SSR)


1. Lỗi không nhắn tin cho chính mình được
2. Frontend lỗi giao diện không đổi được sang người khác (cụ thể là không đổi được receiver_id trên url)
3. Do là nhắn với chính mình nên là đã được tạo trước conversation_id rồi, đường dẫn của nó nên là conversation_id luôn
4. Trong frontend, nếu như đã tồn tạo conversation_id rồi thì cứ lấy conversation_id, còn không thì mới dùng receiver_id


1. Khi url có chữ receiver thì phải check trong cache xem là đã có conversation_id chưa, nếu như có thì replace, nếu như trong cache không có thì call API ở backend, nếu vẫn chưa có thì mới để url là receiver_id

2. Giao diện message sidebar bên trái chỉ chứa cuộc hội thoại đã chứa conversation_id

3. Khi bấm vào tin nhắn mới thì hiện ra list các user, bấm vào 1 người thì hiện ra conversation_id với người đó, nếu như không có thì mới hiện ra receiver_id