# Hướng dẫn lấy CLOUDFLARE_WEBHOOK_SECRET từng bước dễ hiểu

Mã bảo mật `CLOUDFLARE_WEBHOOK_SECRET` được Cloudflare **cấp duy nhất 1 lần** ngay lúc bạn đăng ký URL Webhook thành công. 

Dưới đây là 3 bước siêu đơn giản để lấy nó.

---

## Bước 1: Mở cổng ra Internet (Dùng Ngrok)

Cloudflare là máy chủ trên Internet, nó không thể gọi trực tiếp vào máy tính của bạn (đang chạy ở `localhost:3001` bị giấu sau cục Wifi). Bạn cần dùng Ngrok để tạo đường hầm.

1. Bật Terminal/CMD lên.
2. Gõ lệnh: `ngrok http 3001` *(Thay 3001 bằng port backend của bạn, nếu chưa có Ngrok thì tải tại ngrok.com)*.
3. Copy cái link màu xanh (dòng Forwarding) mà Ngrok cấp cho bạn. 
   *(Ví dụ: `https://4a3b-113.ngrok-free.app`)*

👉 Suy ra, đường link Webhook hoàn chỉnh của bạn sẽ là cái link Ngrok cộng thêm đuôi API: 
`https://4a3b-113.ngrok-free.app/api/message/stream/webhook`

---

## Bước 2: Báo cho Cloudflare biết đường link Webhook của bạn

Vì Cloudflare Stream không có giao diện Web (UI) để dán link Webhook, bạn phải gọi API để báo cho nó biết. Có 2 cách, bạn chọn cách nào thấy dễ hơn:

### Cách 2A: Dùng lệnh `curl` (Nhanh nhất)
Mở một cửa sổ Terminal **mới** (phải giữ nguyên cửa sổ Ngrok đang chạy nhé), sửa thông tin của bạn vào lệnh dưới đây rồi bấm Enter:

```bash
curl -X PUT "https://api.cloudflare.com/client/v4/accounts/<THAY_BẰNG_ACCOUNT_ID>/stream/webhook" \
     -H "Authorization: Bearer <THAY_BẰNG_API_TOKEN>" \
     -H "Content-Type: application/json" \
     -d '{"notificationUrl":"https://4a3b-113.ngrok-free.app/api/message/stream/webhook"}'
```
*(Lưu ý: Nhớ đổi cái link ngrok ở dòng cuối cùng thành link thật của bạn).*

### Cách 2B: Dùng code tự động trong `media.service.ts`
Nếu bạn lười gõ lệnh curl, hãy code thẳng vào hàm `setupCloudflareWebhook` trong `MediaService` rồi gọi chạy nó 1 lần:

```typescript
async setupCloudflareWebhook(webhookUrl: string) {
    const response = await fetch(`https://api.cloudflare.com/client/v4/accounts/${config.cloudflare.account_id}/stream/webhook`, {
        method: 'PUT',
        headers: {
            'Authorization': `Bearer ${config.cloudflare.api_key}`,
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({ notificationUrl: webhookUrl })
    });
    
    const data = await response.json();
    console.log("DỮ LIỆU CLOUDFLARE TRẢ VỀ:", JSON.stringify(data, null, 2));
    return data;
}
```

---

## Bước 3: Lấy Secret Key và bỏ vào `.env`

Sau khi bạn chạy lệnh `curl` (hoặc chạy code) ở Bước 2 thành công, Cloudflare sẽ phản hồi về một cục dữ liệu (JSON) nhìn như thế này:

```json
{
  "result": {
    "notificationUrl": "https://4a3b-113.ngrok-free.app/api/message/stream/webhook",
    "modified": "2026-08-02T...Z",
    "secret": "85011ed3a913c6ad5f9cf6c5573cc0a7"  <--- ĐÂY CHÍNH LÀ NÓ!
  },
  "success": true,
  "errors": [],
  "messages": []
}
```

Bạn chỉ cần:
1. Copy chuỗi chữ đằng sau chữ `"secret"` (Trong ví dụ trên là `85011ed3a913c6ad5f9cf6c5573cc0a7`).
2. Mở file `.env` của Backend lên.
3. Dán nó vào cuối file:
   ```env
   CLOUDFLARE_WEBHOOK_SECRET=85011ed3a913c6ad5f9cf6c5573cc0a7
   ```

🎉 **XONG!** Bây giờ hệ thống backend của bạn đã có mã bí mật này để đối chiếu xem Webhook gửi tới có đúng là của Cloudflare hay là do hacker giả mạo.

> ⚠️ **Lưu ý quan trọng lúc code ở Local:** 
> Ngrok bản miễn phí sẽ tự đổi link mỗi lần bạn tắt máy bật lại. Do đó, **mỗi khi bật lại Ngrok**, bạn phải làm lại Bước 2 để đăng ký link mới với Cloudflare. Cloudflare sẽ cấp lại cho bạn một mã `secret` mới, và bạn lại copy dán đè vào `.env` nhé! Khi nào up web lên Server thật có tên miền cố định thì mới không phải làm lại.
