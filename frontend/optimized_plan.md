# KẾ HOẠCH TỐI ƯU HÓA TOÀN DIỆN HIỆU NĂNG FRONTEND (PERFORMANCE OPTIMIZATION PLAN)

> **NGUYÊN TẮC BẤT DI BẤT DỊCH:** Toàn bộ logic nghiệp vụ (business logic), cấu trúc kiến trúc Fractal Component (`.tsx` + `use*.ts`), Single Realtime Core (Socket Singleton), và các quy tắc hiển thị/kiểm tra quyền **BẮT BUỘC PHẢI GIỮ NGUYÊN 100%**. Kế hoạch tối ưu chỉ tập trung vào: **Bộ nhớ đệm (Persistence Cache)**, **Hiệu suất Render (Virtualization & Memo)**, **Kích thước gói tải (Code Splitting/Bundle Size)** và **Độ trễ cảm nhận (Optimistic UI)**.

---

## MỤC TIÊU HIỆU NĂNG (TARGET METRICS)

1. **First Contentful Paint (FCP) & Offline-First:** Giảm thời gian tải trang từ ~1.2s xuống **< 200ms** khi mở lại ứng dụng (dữ liệu hội thoại và tin nhắn hiển thị tức thì từ Cache).
2. **First Load JS Bundle:** Giảm **~700KB - 1MB** dung lượng tải trang lần đầu bằng cách tách tải động (Dynamic Import) các thư viện nặng.
3. **Giảm thiểu Request HTTP:** Cắt giảm **85% - 90%** số lượng request gọi API batch user (`POST /api/user/batch`) và tải lại danh sách hội thoại.
4. **Rendering 60 FPS:** Triệt tiêu hiện tượng giật lag khi phòng chat có từ 500 đến 10.000 tin nhắn bằng cơ chế Virtual Scrolling, giảm 95% re-render dư thừa.
5. **Độ trễ gửi tin (Perceived Latency):** Phản hồi tin nhắn trên màn hình tức thì trong **0ms** (Optimistic UI) thay vì chờ đợi round-trip từ mạng.

---

## MỤC 1: TỐI ƯU GÓI TẢI (BUNDLE SIZE & LAZY LOADING)

### 1.1. Tách tải động bảng chọn Emoji (`@emoji-mart/data` & `@emoji-mart/react`)
* **Vấn đề:** Trong `ChatInput.tsx`, `@emoji-mart/data` (~700KB JSON) và `@emoji-mart/react` được import tĩnh. Người dùng chưa mở emoji vẫn phải tải toàn bộ dữ liệu này ngay từ lần đầu vào trang chat.
* **Giải pháp:**
  - Áp dụng `next/dynamic` kết hợp import động khi mở:
    ```tsx
    const EmojiPicker = dynamic(() => import('@emoji-mart/react'), {
      ssr: false,
      loading: () => (
        <div className="w-[352px] h-[435px] flex items-center justify-center bg-surface border border-glass-border rounded-2xl">
          <Loader2 className="w-6 h-6 animate-spin text-brand-primary" />
        </div>
      ),
    });
    ```
  - Chỉ nạp `@emoji-mart/data` khi người dùng click vào nút icon mặt cười lần đầu tiên.
* **Hiệu quả:** Giảm ngay **~700KB** dung lượng bundle của `ChatInput`.

### 1.2. Lazy loading các Modal dung lượng lớn (Code Splitting Modals)
* **Vấn đề:** Các modal như `MediaViewerModal`, `DisbandGroupModal`, `AssignAdminModal`, `KickMemberModal`, `EditGroupModal` đều được nạp sẵn vào component cha dù rất ít khi được kích hoạt.
* **Giải pháp:** Sử dụng `React.lazy` hoặc `next/dynamic` cho các Modal con. Chỉ tải chunk mã nguồn của modal khi state `showModal === true`.
* **Hiệu quả:** Giảm tải bộ nhớ ban đầu và tăng tốc độ mount component.

---

## MỤC 2: TỐI ƯU BỘ NHỚ ĐỆM & GIẢM THIỂU GỌI API (PERSISTENCE CACHE)

### 2.1. Persist `userStore` (Bộ nhớ đệm Profile Sinh Viên)
* **Vấn đề:** `useUserStore` quản lý `users: Record<string, User>` trong RAM. Cứ mỗi lần người dùng F5 hoặc mở lại ứng dụng, toàn bộ danh bạ sinh viên trong bộ đệm bị xóa sạch $\rightarrow$ Hệ thống lại phải gom ID và gọi hàng loạt request `POST /api/user/batch`.
* **Giải pháp:**
  - Bọc Zustand store bằng middleware `persist` lưu trữ vào `localStorage`:
    ```typescript
    export const useUserStore = create<UserStore>()(
      persist(
        (set, get) => ({
          users: {},
          // ... giữ nguyên toàn bộ logic DataLoader hiện tại
        }),
        {
          name: "hcmus_users_cache",
          storage: createJSONStorage(() => localStorage),
          partialize: (state) => ({ users: state.users }), // Chỉ lưu cache users
        }
      )
    );
    ```
  - Tích hợp cơ chế dọn dẹp (TTL / Max Size): Giới hạn tối đa 500 profile gần nhất để không làm đầy `localStorage` (dung lượng 500 user profile chỉ ~150KB).
* **Hiệu quả:** Giảm **85% - 90%** số lượng request HTTP batch users. Bạn bè và thành viên nhóm hiển thị tên và avatar ngay lập tức khi mở app.

### 2.2. Persist React Query Cache vào IndexedDB (`@tanstack/react-query-persist-client`)
* **Vấn đề:** Dữ liệu hội thoại (`conversations`) và tin nhắn (`messages`) được lưu trong React Query Cache trên RAM. Mất kết nối mạng hoặc F5 sẽ làm mất toàn bộ dữ liệu tạm thời này.
* **Giải pháp:**
  - Cài đặt `@tanstack/react-query-persist-client` kết hợp `idb-keyval` (sử dụng IndexedDB không giới hạn 5MB như `localStorage`).
  - Cấu hình Persister:
    ```typescript
    const persister = createAsyncStoragePersister({
      storage: {
        getItem: async (key) => await get(key),
        setItem: async (key, val) => await set(key, val),
        removeItem: async (key) => await del(key),
      },
    });
    ```
  - Thiết lập `maxAge: 24 * 60 * 60 * 1000` (24 giờ).
* **Hiệu quả:** Trải nghiệm **Offline-First**. Vừa mở web lên, danh sách phòng chat và toàn bộ tin nhắn gần nhất hiện lên ngay tức khắc mà không cần đợi API mạng phản hồi.

---

## MỤC 3: TỐI ƯU RENDER & VIRTUALIZATION (RENDERING PERFORMANCE)

### 3.1. Bọc `React.memo` cho `MessageItem`
* **Vấn đề:** Khi có người dùng đang gõ phím (`currentTypingUsers` thay đổi), hoặc khi có tin nhắn mới rơi vào cuối danh sách, `MessageList` re-render và khiến toàn bộ 50 - 100 component `MessageItem` trước đó đều bị re-render không cần thiết.
* **Giải pháp:**
  - Bọc `MessageItem` bằng `React.memo`:
    ```typescript
    export const MessageItem = React.memo(
      MessageItemComponent,
      (prevProps, nextProps) => {
        return (
          prevProps.message.id === nextProps.message.id &&
          prevProps.message.content === nextProps.message.content &&
          prevProps.message.updated_at === nextProps.message.updated_at &&
          prevProps.message.is_recalled === nextProps.message.is_recalled &&
          prevProps.isLastMessage === nextProps.isLastMessage &&
          JSON.stringify(prevProps.watermarks) === JSON.stringify(nextProps.watermarks) &&
          prevProps.message.reactions?.length === nextProps.message.reactions?.length
        );
      }
    );
    ```
* **Hiệu quả:** Giảm **95%** số lượng render thừa trong danh sách tin nhắn.

### 3.2. Virtual Scrolling cho Danh sách Tin nhắn (`@tanstack/react-virtual`)
* **Vấn đề:** Nếu một cuộc hội thoại kéo dài tải 500 - 1.000 tin nhắn, toàn bộ DOM nodes (chứa text, ảnh, video player, reaction pills, avatar) đều nằm trên cây DOM thật $\rightarrow$ Tốn dung lượng RAM trình duyệt và gây giật lag khi cuộn nhanh.
* **Giải pháp:**
  - Sử dụng thư viện `@tanstack/react-virtual` để ảo hóa danh sách cuộn ngược:
    - Chỉ render khoảng **15 - 20 phần tử thực tế** nằm trong khung nhìn (Viewport).
    - Các phần tử ngoài khung nhìn được thay thế bằng padding giả lập chiều cao.
* **Hiệu quả:** DOM luôn gọn nhẹ (< 100 nodes), tốc độ cuộn đạt chuẩn mượt mà **60 FPS** với mọi độ dài lịch sử chat.

---

## MỤC 4: TRẢI NGHIỆM GỬI TIN 0MS (OPTIMISTIC UI UPDATES)

* **Vấn đề:** Hiện tại khi nhấn Enter gửi tin nhắn, input phải đợi tín hiệu phản hồi từ Server/WebSocket rồi mới chèn tin nhắn vào danh sách $\rightarrow$ Tạo cảm giác có độ trễ nhẹ nếu mạng chậm.
* **Giải pháp:**
  1. Khi người dùng bấm Gửi trong `useChatInput.ts`:
     - Tạo ngay một tin nhắn giả lập cục bộ:
       ```typescript
       const optimisticMessage: Message = {
         id: `temp_${Date.now()}`,
         conversation_id: activeConversation.id,
         sender_id: user.id,
         content: currentContent,
         type: 'text',
         created_at: new Date().toISOString(),
         status: 'sending', // Trạng thái đang gửi (hiển thị icon đồng hồ)
       };
       ```
     - Gọi `chatCache.appendNewMessage(queryClient, optimisticMessage)` ngay lập tức ($0ms$).
     - Xóa rỗng ô soạn thảo lập tức.
  2. Gửi sự kiện Socket qua `messageApi.sendMessage(payload)`.
  3. Khi nhận kết quả ACK thành công từ server:
     - Cập nhật tin nhắn tạm trong cache thành tin nhắn thật với ID từ server và `status: 'sent'`.
  4. Nếu có lỗi mạng hoặc timeout:
     - Đổi `status: 'error'` và hiển thị icon cảnh báo màu đỏ kèm nút **Gửi lại (Retry)**.
* **Hiệu quả:** Mang lại trải nghiệm nhắn tin thời gian thực mượt mà và nhạy bén tương đương Telegram/Messenger.

---

## MỤC 5: TỐI ƯU NETWORK & WATERMARKS

### 5.1. Throttle / Gom nhóm sự kiện `mark_read`
* **Vấn đề:** Khi mở một box chat có 10 tin nhắn mới dồn dập, client có thể phát sinh nhiều lệnh `mark_read` riêng rẽ lên Socket Server.
* **Giải pháp:** Gom nhóm và chỉ phát duy nhất một sự kiện `mark_read` với ID của tin nhắn mới nhất trong viewport sau khoảng debounce/throttle **300ms**.
* **Hiệu quả:** Giảm tải số lượng event gửi lên Redis / Worker ở phía Backend.

### 5.2. Tối ưu nén ảnh Thumbnail trước khi Upload
* Tích hợp thư viện nén ảnh nhanh phía client (`browser-image-compression`) trước khi gọi `uploadService.uploadImage`. Tự động nén các ảnh dung lượng lớn (> 5MB) xuống còn < 1MB nhưng vẫn giữ nguyên độ sắc nét mắt thường.

---

## LỘ TRÌNH THỰC HIỆN ĐỀ XUẤT (IMPLEMENTATION ROADMAP)

### Giai đoạn 1: Quick Wins (Tác động lớn, triển khai nhanh)
- [ ] **Bước 1:** Áp dụng `next/dynamic` cho Emoji Picker trong `ChatInput.tsx` (Giảm ngay ~700KB bundle).
- [ ] **Bước 2:** Bổ sung `persist` cho `useUserStore` vào `localStorage` (Cắt giảm 85% request batch users).
- [ ] **Bước 3:** Bọc `React.memo` cho `MessageItem.tsx` (Triệt tiêu re-render khi typing/nhận tin).

### Giai đoạn 2: Trải nghiệm người dùng cao cấp (Perceived Performance)
- [ ] **Bước 4:** Triển khai **Optimistic UI Update** khi gửi tin nhắn trong `useChatInput.ts` (Phản hồi 0ms).
- [ ] **Bước 5:** Tách dynamic import cho các Modal hiếm dùng (`MediaViewerModal`, `DisbandGroupModal`, v.v.).

### Giai đoạn 3: Scale dữ liệu lớn (Dành cho phòng chat hàng chục nghìn tin)
- [ ] **Bước 6:** Tích hợp `@tanstack/react-virtual` vào `MessageList.tsx`.
- [ ] **Bước 7:** Tích hợp `@tanstack/react-query-persist-client` với IndexedDB (`idb-keyval`).


## 13. RÀ SOÁT CÁC VỊ TRÍ SỬ DỤNG TRỰC TIẾP `queryClient` NGOÀI HANDLERS & ĐỀ XUẤT TỐI ƯU

### 13.1. Nguyên Tắc Thiết Kế (Architectural Guidelines)
Theo kiến trúc Single Source of Truth và Encapsulation, **`queryClient` không nên bị gọi tùy tiện ở các UI Components hay Controller Hooks** để `setQueryData` / `getQueriesData` thủ công. Toàn bộ logic can thiệp vào cấu trúc in-memory cache của TanStack Query phải được đóng gói tập trung vào file tiện ích [`chat-cache.util.ts`](file:///Users/ductri0981/Documents/Chatbot-HCMUS/frontend/src/features/chat/utils/chat-cache.util.ts) (`chatCache`).

### 13.2. Danh Sách Các Vị Trí Dùng Trực Tiếp `queryClient` (Ngoài file `.handler.ts` và `chat-cache.util.ts`)

| STT | File | Dòng | Mục đích hiện tại | Đánh giá kiến trúc | Đề xuất hướng sửa |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **1** | `src/features/chat/components/ChatArea/components/ConversationInfo/ConversationInfo.tsx` | 181 | Tự viết hàm `updateQueryCache` lặp qua các `pages` để cập nhật thông tin nhóm sau khi đóng `EditGroupModal`. | **Vi phạm nguyên tắc**. Duplicate logic và tự can thiệp raw cache ở tầng Presentation Component. | Thay bằng hàm có sẵn `chatCache.updateConversationInfo(queryClient, updatedConv)` hoặc để socket `conversation_updated` tự xử lý. |
| **2** | `src/features/chat/components/ChatArea/components/ConversationInfo/useConversationInfo.ts` | 19, 254 | Khởi tạo `useQueryClient()` và export ra đối tượng return `queryClient`. | **Thừa thãi**. Chỉ tồn tại để truyền `queryClient` xuống cho `ConversationInfo.tsx` ở mục 1. | Xóa bỏ `useQueryClient()` và không return `queryClient` trong hook này nữa. |
| **3** | `src/features/chat/components/Messages/MessageList/useMessageList.ts` | 15, 112, 137 | Khi click vào kết quả tìm kiếm tin nhắn, hook gọi `queryClient.setQueryData(["messages", convId], ...)` để ghi đè danh sách tin nhắn ngữ cảnh. | **Cần đóng gói**. Hook UI đang can thiệp trực tiếp vào cấu trúc phân trang `{ pages, pageParams }` của tin nhắn. | Tạo hàm `chatCache.setContextMessages(queryClient, convId, messages)` trong `chat-cache.util.ts` và gọi qua hàm này. |
| **4** | `src/features/chat/hooks/useChatScreen.ts` | 16, 42, 68, 109 | Gọi `queryClient.getQueriesData({ queryKey: ['conversations'] })` để lặp qua cache tìm conversation theo `cId` hoặc `receiverId` nhằm set `activeConversation` tức thì. | **Trùng lặp code**. Logic duyệt raw cache `{ pages: Conversation[][] }` bị lặp lại ở nhiều nơi. | Tạo hàm helper `chatCache.findConversationInCache(queryClient, convIdOrReceiverId)` để tái sử dụng. |
| **5** | `src/features/chat/components/Sidebar/components/FriendBar/components/Search/useSearchItem.ts` | 19, 33, 63 | Gọi `queryClient.getQueriesData({ queryKey: ['conversations'] })` để kiểm tra xem đã có cuộc trò chuyện 1-1 với user được tìm kiếm hay chưa. | **Trùng lặp code**. Lặp lại y hệt logic đọc cache của `useChatScreen.ts`. | Dùng chung hàm helper `chatCache.findUtuConversation(queryClient, targetUserId)` từ `chatCache`. |
| **6** | `src/features/chat/socket/useChatSocket.ts` | 15, 33-35, 44 | Gọi `useQueryClient()` để tiêm `queryClient` vào các socket handlers (`registerMessageHandlers`, `registerConversationHandlers`, `registerWatermarkHandlers`). | **Hợp lệ (Điểm tiêm phụ thuộc)**. Đây là cầu nối trung gian duy nhất giữa React Context và Socket Handlers. | Giữ nguyên. |
| **7** | `src/providers/QueryProvider.tsx` | 8, 21 | Khởi tạo `new QueryClient()` và bọc `<QueryClientProvider client={queryClient}>` ở Root Layout. | **Hợp lệ (Root Config)**. | Giữ nguyên. |

---

### 13.3. Đề Xuất Chi Tiết Cách Refactor Cho Từng Vị Trí

#### 1. Tại `ConversationInfo.tsx`:
* **Hiện tại (Dòng 180 - 196):**
```tsx
const updateQueryCache = (queryKey: any[]) => {
  info.queryClient.setQueryData(queryKey, (oldData: any) => {
    if (!oldData || !oldData.pages) return oldData;
    return {
      ...oldData,
      pages: oldData.pages.map((page: any[]) =>
        page.map((conv: any) =>
          conv.id === updatedConv.id ? { ...conv, ...updatedConv } : conv
        )
      ),
    };
  });
};
```
* **Đề xuất sửa:**
Xóa bỏ hoàn toàn hàm `updateQueryCache` tự viết trên. Vì khi cập nhật nhóm thành công, backend đã tự động emit socket `conversation_updated`, và [`conversation.handler.ts`](file:///Users/ductri0981/Documents/Chatbot-HCMUS/frontend/src/features/chat/socket/handlers/conversation.handler.ts#L95-L97) đã tự động cập nhật cache thông qua `chatCache.updateConversationInfo(queryClient, updatedConv)`.
Nếu muốn Optimistic update ngay lập tức tại chỗ trước khi socket phản hồi:
```tsx
onSuccess={(updatedConv) => {
  chatCache.updateConversationInfo(queryClient, updatedConv);
  info.setShowEditGroupModal(false);
}}
```

#### 2. Tại `useMessageList.ts`:
* **Hiện tại (Dòng 112 - 117):**
```typescript
queryClient.setQueryData(["messages", convId], () => {
  return {
    pages: [contextMessages],
    pageParams: [undefined],
  };
});
```
* **Đề xuất sửa:**
Đóng gói vào `chatCache` trong `chat-cache.util.ts`:
```typescript
// Thêm vào chatCache:
setContextMessages: (queryClient: QueryClient, conversationId: string, messages: Message[]) => {
  queryClient.setQueryData(["messages", conversationId], {
    pages: [messages],
    pageParams: [undefined],
  });
}
```
Tại `useMessageList.ts` chỉ cần gọi:
```typescript
chatCache.setContextMessages(queryClient, convId, contextMessages);
```

#### 3. Tại `useChatScreen.ts` & `useSearchItem.ts`:
* **Hiện tại:** Cả hai file đều tự viết vòng lặp duyệt cache:
```typescript
const allCaches = queryClient.getQueriesData<{ pages: Conversation[][] }>({ queryKey: ['conversations'] });
for (const [, cacheData] of allCaches) {
  for (const page of cacheData.pages) {
    // ... tìm conversation
  }
}
```
* **Đề xuất sửa:**
Tạo 2 hàm truy vấn cache tập trung trong `chatCache`:
```typescript
// Thêm vào chatCache:
findConversationById: (queryClient: QueryClient, conversationId: string): Conversation | undefined => {
  const allCaches = queryClient.getQueriesData<{ pages: Conversation[][] }>({ queryKey: ['conversations'] });
  for (const [, cacheData] of allCaches) {
    if (!cacheData?.pages) continue;
    for (const page of cacheData.pages) {
      const found = page.find((c) => c.id === conversationId);
      if (found) return found;
    }
  }
  return undefined;
},

findUtuConversationByMemberId: (queryClient: QueryClient, targetUserId: string): Conversation | undefined => {
  const allCaches = queryClient.getQueriesData<{ pages: Conversation[][] }>({ queryKey: ['conversations'] });
  for (const [, cacheData] of allCaches) {
    if (!cacheData?.pages) continue;
    for (const page of cacheData.pages) {
      const found = page.find((c) => c.type === 'utu' && c.member_ids?.includes(targetUserId));
      if (found) return found;
    }
  }
  return undefined;
}
```
Khi đó, trong `useChatScreen.ts` và `useSearchItem.ts` chỉ cần gọi:
```typescript
const existing = chatCache.findConversationById(queryClient, cId);
// hoặc
const existingUtu = chatCache.findUtuConversationByMemberId(queryClient, receiverId);
```
Code trở nên sáng sủa, sạch sẽ, không bị lặp lại cấu trúc `pages` của TanStack Query ở bất kỳ file view/hook nào.
