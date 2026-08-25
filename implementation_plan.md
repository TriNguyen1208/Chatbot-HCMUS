6. Trạng thái online (Cái này thì check trạng thái trong socket)
7. Thiết kế lại socket join conversation khi vừa mới vào (không được phép mặc định limit 100 người)
8. Tìm kiếm với thanh search
9. Làm xem chi tiết 1 cuộc trò chuyện
10. Làm cập nhật profile


Trong database phải cập nhật trạng thái online gần nhất. (Khi mất kết nối với s)


Cái join conversation khi mới vào:
Bài toán: Hãy thiết kế 1 thuật toán join conversation sao cho khi người dùng mới vào thì chỉ join 1 conversation được chọn. Đừng join hết các conversation 

Hiện tại: Join các conversation với limit bằng 100 (Khá lỏ)

Hệ thống nên nhận diện được conversation chung của từng user trong socket (ví dụ như A với B chung 1 conversation thì lúc B mới vào thì cho B join conversation và A join conversation)

TH1: nếu như A là người vào đầu tiên, A không cần phải join conversation nào. Khi mà gửi tin nhắn đầu tiên (hàm gửi tin nhắn phải check nếu như chưa join conversation thì phải join conversation)

TH2: nếu như A là người vào thứ 2, thì phải check xem A với tất cả người có kết nối với A đang online, nếu như có thì phải join conversation cả 2 thằng với A và B chung 1 conversation (check trong database)

TH3: nếu như A là người vào thứ 2, và A kết nối với B, nhưng B đang offline thì A không cần phải join conversation nào hết. (Nhưng lúc gửi tin nhắn thì vẫn phải join conversation)


Hướng xử lý: 
- Khi A vừa mới vào: on(connection), thì hãy check xem trong socket có thằng nào đang online không: Nếu có thì check xem mấy thằng online đó có kết nối với A không, nếu có thì join conversation với các thằng đó (cả 2 đều phải join conversation). Nếu không thì skip (nhưng lúc gửi tin nhắn vẫn phải join conversation (để emitGroup lúc gửi tin nhắn frontend nhận được new_message))


Hoặc là phương pháp khác, không cần join conversation lúc đầu luôn. Nếu như A gửi cho B ở conversation_id là con1, thì lúc đó A và B mới cùng join conversation (nếu chưa có), còn không thì sẽ không join conversation nào cả. Như vậy thì sẽ không có trường hợp join hết conversation 