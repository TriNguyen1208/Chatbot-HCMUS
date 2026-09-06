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



Giao diện thanh search ở frontend:
+ Bấm vào thanh search là đổi giao diện sidebar
+ Khi bấm vào thì hiện ra các option: All, User, Conversation, Message

+ Khi bấm vào User thì khi search hiện ra name và avatar_url
+ Khi bấm vào Conversation thì hiện ra name và avatar_url (của group)
+ Khi bấm vào message (message tất cả không dựa theo conversation) thì hiện ra content, người gửi tin nhắn đó (sender_id) và cả cuộc hội thoại chứa tin nhắn đó (gồm có name và avatar của cuộc hội thoại). (Lúc search ở backend thì lấy ra thêm) (Sửa backend chỗ này)
+ Khi bấm vào all thì dựa vào search_types mà hiện ra giống với User, Conversation, Message

+ Khi bấm vào All thì dựa vào search_type để hiện ra giao diện, nếu là user thì hiện ra giao diện có name và avatar_url, nếu search_type là conversation thì hiện ra name (tên nhóm và avatar_url), nếu là message thì hiện ra nội dung tin nhắn chào, người gửi tin nhắn đó (sender_id), cuộc hội thoại chứa tin nhắn đó (gồm có name và avatar của cuộc hội thoại).