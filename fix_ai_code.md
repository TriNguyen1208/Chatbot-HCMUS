### Lỗi 1
- Đoạn code này hơi lạ, khi mà ở trên client.sadd rồi thì đương nhiên count === 1, làm gì xảy ra trường hợp count !== 1?
- Ý tưởng: Khúc online thì không cần phải update database last_active, khi nào offline mới update
```typescript
async userConnect(userID: string, socketID: string): Promise<boolean> {
    const client = redisClient.getClient();
    await client.sadd(`presence:sockets:${userID}`, socketID);
    await client.set(`presence:heartbeat:${userID}`, "1", "EX", 45); // Heartbeat with TTL

    const count = await client.scard(`presence:sockets:${userID}`);
    if (count === 1) {
        // Cập nhật last_active là thời điểm hiện tại khi online (Tuỳ chọn, nhưng giúp record thời điểm bắt đầu session)
        await this.update(userID, { last_active: new Date().toISOString() });
        return true; 
    }
    return false;
}
```

### Lỗi 2
Tương tự như vậy, chắc chắn count == 0, không thể khác 0 nên không cần phải if
```typescript
async userDisconnect(userID: string, socketID: string): Promise<boolean> {
    const client = redisClient.getClient();
    await client.srem(`presence:sockets:${userID}`, socketID);
    const count = await client.scard(`presence:sockets:${userID}`);
    
    if (count === 0) {
        // Delete heartbeat
        await client.del(`presence:heartbeat:${userID}`);
        // Save last_active
        const lastActive = new Date().toISOString();
        await this.update(userID, { last_active: lastActive });
        return true; // Just went offline
    }
    return false;
}
```

### Lỗi 3: Xem lại socket-manager.ts