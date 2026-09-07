8. Tìm kiếm với thanh search


8. ý tưởng:
- Trong elastic seach lưu 3 indices là message, user và conversation
- Khi search trên thanh search chuẩn thì có thể có 3 TH:
    - TH1: Tên MSSV hoặc tên hoặc gmail hoặc sđt thì search theo index của user (Trả về user_id)
    - TH2: Search theo tên conversation (tên nhóm) thì search theo conversation name của conversation (trả về conversation_id)
    - TH3: Search theo nội dung tin nhắn
    => Sau đó push user_id và conversation_id vào message đó để thực hiện search    
    => Tất cả đều phải trả ra được conversation_id đó và gửi response gồm có search_type và conversation_id
- Khi search trong tin nhắn thì chỉ search theo trường tin nhắn và trả về message_id đó, bấm vào thì tự động scroll lên đúng tin nhắn đó

- Lưu ý: Dùng rabbitMQ - message queue để sync giữa database và elasticsearch. Không nên dùng đồng bộ trong tình huống này, giảm latency khá mạnh do là vừa phải cập nhật database (mongoDB) vừa phải thêm vào elastic search.


- Làm về trạng thái đã gửi, đã nhận, đã xem
- Đã gửi: Server đã nhận tin nhắn và lưu vào mongoDB, đồng thời bắn socket - nếu socket đó không có user đó thì là đã gửi
- Đã nhận: Client đó đang online thì là đã nhận
- Đã xem: Client đó đã nhận và có bắn gói tin ack đã có tin nhắn đó thì là đã xem