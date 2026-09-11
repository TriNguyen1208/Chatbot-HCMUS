5. Hiện tại lỗi delivered, read,...
7. Refactor lại toàn bộ cấu trúc frontend và backend (Phải tối ưu)
8. Thêm AI Chat vào cả trò chuyện riêng với AI và tích hợp AI vào cuộc hội thoại
9. Sửa SEO cho trang web (SSR)

Giao diện bị khác:
+ Màu của tin nhắn trong khung trò chuyện (phần last_message) khi đang ở trạng thái "Đã gửi" bị đậm hơn, không phải mờ giống ban đầu
+ Mất mấy cái chữ "Bạn", "Tên đối phương" để phân biệt ai là người gửi ở phần sidebar (tin nhắn last message)
+ Avatar ở phần seen tin nhắn bo tròn thêm (hiện tại chưa được bo tròn nếu như ảnh có height > width)
+ Cấp quyền admin, Xoá thành viên hay thêm thành viên đều có socket, vậy mà ở phần (thành viên đoạn chat không cập nhật)