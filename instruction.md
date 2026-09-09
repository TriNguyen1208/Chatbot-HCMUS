
3. Chỗ chỉnh sửa group và thêm cho tôi chỉnh sửa với utu thêm primary icon (Ở đây thì list các icon giống như lúc chat)
+ Lưu ý nút chỉnh sửa conversation với 'utu' cũng giống với 'group', chỉ khác là chỉ cho phép sửa thêm primary_icon
+ Backend chỗ update conversation thì thêm body là có thêm primary_icon nữa
+ Ở frontend thì hiện ra primary_icon ở giữa nút reaction và nút send message
+ Việc update primary_icon thì dựa vào các icon của reaction


4. Dùng MSSV để lọc ra người dùng là khoa gì, chương trình gì (sẽ có file config sẵn ở backend)
+ Trong file config của backend thêm 1 list các khoa dựa vào MSSV, cái này thì lấy trên facebook - mapping mssv ra được role
+ Ở user thì thêm trường role (Cái này tạo trong lúc login luôn)
+ Thay vì hiện active researcher thì hiện ra khoa và chương trình (Ví dụ: Khoa CNTT - Chương trình Kỹ sư phần mềm) còn không thì hiện ra Khách
5. Hiện tại lỗi delivered, read,...

6. Thêm việc gửi link
7. Refactor lại toàn bộ cấu trúc frontend và backend (Phải tối ưu)
8. Thêm AI Chat vào cả trò chuyện riêng với AI và tích hợp AI vào cuộc hội thoại
9. Sửa SEO cho trang web (SSR)