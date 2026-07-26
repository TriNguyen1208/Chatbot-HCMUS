# Hướng Dẫn Di Chuyển (Migration) & Áp Dụng Kiến Trúc Đa CSDL (Polyglot Persistence)

Kiến trúc mới sử dụng Interface `IDatabase` cho phép ứng dụng của bạn chạy **song song cả MongoDB (NoSQL) và Supabase PostgreSQL (SQL)**. Các Repositories và Services sẽ không cần biết cụ thể chúng đang nói chuyện với loại CSDL nào.

Dưới đây là 5 bước để tổ chức lại thư mục và thực hiện chuyển đổi một cách sạch sẽ nhất:

---

## Bước 1: Khởi tạo kết nối cả 2 Database
Hiện tại bạn đang gọi `mongoDB.connect()` trong `src/index.ts`. Hãy đảm bảo rằng bạn kết nối cả Supabase nếu muốn dùng nó.

Trong `src/index.ts`:
```typescript
import { mongoDB } from "#@/shared/database/mongoDBAtlas.js"
import { supabaseDB } from "#@/shared/database/supabaseClient.js"
import { redisClient } from "#@/shared/database/redis.js"

const start = async(): Promise<void> => {
    // Khởi động đồng thời cả Mongo và Postgres
    await Promise.all([
        mongoDB.connect(),
        supabaseDB.connect(),
        redisClient.connect()
    ]);
    
    // ... start app
}
```

---

## Bước 2: Đổi tên thư mục `models` thành `entities`
Xóa bỏ khái niệm "Model", thay vào đó hãy dùng `entities/`. Đây là nơi chứa định nghĩa (cấu trúc dữ liệu) cho TẤT CẢ các CSDL. 

### 1. Đối với bảng lưu trên PostgreSQL (Ví dụ: `User`)
File entity của Postgres sẽ cực kỳ mỏng nhẹ vì Postgres đã tự quản lý schema dưới CSDL rồi. Nó chỉ chứa TypeScript Interface:

Tạo file: `/src/modules/user/entities/user.entity.ts`
```typescript
// Định nghĩa kiểu dữ liệu cho bảng 'users' trong Supabase (Postgres)
export interface UserEntity {
    id: string; // Khóa chính (UUID)
    email: string;
    name: string;
    student_id?: string;
    phone?: string;
    avatar_url?: string; // Tên biến chuẩn snake_case
    created_at: Date;
    updated_at: Date;
}
```

### 2. Đối với Collection lưu trên MongoDB (Ví dụ: `Conversation`)
File entity của Mongo sẽ chứa cả Interface và cấu hình (Schema) của Mongoose.

Tạo file: `/src/modules/chat/entities/conversation.entity.ts`
```typescript
import mongoose, { Schema, Document } from "mongoose";

// 1. Interface
export interface ConversationEntity extends Document {
    user_id: string; // ID lấy từ Postgres
    title: string;
    messages: any[];
}

// 2. Schema cấu hình Mongoose
const ConversationSchema = new Schema<ConversationEntity>({
    user_id: { type: String, required: true },
    title: { type: String, required: true },
    messages: []
});

export const ConversationModel = mongoose.model<ConversationEntity>('Conversation', ConversationSchema);
```

---

## Bước 3: Viết lại Repository (Refactoring)
Bây giờ `UserRepository` không gọi trực tiếp `UserModel` nữa, mà nhận `IDatabase` và xài generic là `UserEntity` vừa tạo ở bước 2.

**Ví dụ sửa file `/src/modules/user/repositories/user.repository.ts`:**

```typescript
import type { UserProfile } from "#@/shared/types/index.js";
import { IDatabase } from "#@/shared/database/database.interface.js";
import { UserEntity } from "../entities/user.entity.js"; // IMPORT ENTITY

export class UserRepository implements IUserRepository {
    // 1. Nhận DB từ bên ngoài truyền vào
    constructor(private readonly db: IDatabase) {}

    // Hàm chuyển đổi từ Entity Database -> Profile trả về Client
    private toProfile(row: UserEntity): UserProfile {
        return {
            id:        row.id,
            email:     row.email,
            name:      row.name,
            student_id: row.student_id,
            phone:     row.phone,
            avatarUrl: row.avatar_url,
            createdAt: row.created_at,
            updatedAt: row.updated_at,
        };
    }

    async findByEmail(email: string): Promise<UserProfile | null> {
        // 2. Dùng this.db.findOne với kiểu <UserEntity>
        const row = await this.db.findOne<UserEntity>('users', { email: email.toLowerCase() });
        return row ? this.toProfile(row) : null;
    }

    async create({ email, name, avatarUrl, student_id }: any): Promise<UserProfile> {
        // 3. Dùng this.db.insert
        const newRow = await this.db.insert<UserEntity>('users', {
            email, name, avatar_url: avatarUrl, student_id
        });
        return this.toProfile(newRow as UserEntity); 
    }
}
```

---

## Bước 4: Dependency Injection - Quyết định DB lúc khởi tạo
Đây là bước "phép màu". Tại file cài đặt route hoặc Controller/Service, bạn quyết định xem Repo nào xài Postgres, Repo nào xài Mongo.

```typescript
import { mongoDB } from '#@/shared/database/mongoDBAtlas.js';
import { supabaseDB } from '#@/shared/database/supabaseClient.js';
import { UserRepository } from '#@/modules/user/repositories/user.repository.js';
import { AuthService } from '#@/modules/user/services/auth.service.js';

// Khởi tạo Repository và TRUYỀN ĐÚNG LOẠI DATABASE MONG MUỐN
// - User cần quan hệ và bảo mật cao => Dùng PostgreSQL (supabaseDB)
const userRepository = new UserRepository(supabaseDB);

// - Lịch sử chat JSON khổng lồ => Dùng MongoDB (mongoDB)
// const conversationRepository = new ConversationRepository(mongoDB);

// Service hoàn toàn không biết sự tồn tại của Mongo hay Postgres
const authService = new AuthService(userRepository, keyStoreRepository);
```

---

## Bước 5: Tạo Bảng trên Supabase (Database Schema)
Hãy nhớ: Không giống MongoDB (tự động tạo schema), với Supabase (PostgreSQL), bạn **bắt buộc phải tạo Table và Column trước** trên giao diện SQL của Supabase.

Vào Supabase Dashboard, chạy lệnh SQL để khởi tạo bảng tương ứng với `UserEntity`:

```sql
CREATE TABLE public.users (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  email TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  student_id TEXT,
  phone TEXT,
  avatar_url TEXT,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```
