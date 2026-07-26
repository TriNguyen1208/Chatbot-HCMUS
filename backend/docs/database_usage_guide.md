# Hướng Dẫn Sử Dụng Database Interface (`IDatabase`)

Tài liệu này hướng dẫn chi tiết cách sử dụng kiến trúc Database Interface mới nhất được thiết kế theo chuẩn **Repository Pattern** và **Dependency Inversion** với **TypeScript Strict Generics**. 

Hệ thống hiện tại hỗ trợ 2 hệ quản trị cơ sở dữ liệu hoàn toàn khác biệt:
1. **MongoDB Atlas** (NoSQL Document - thông qua Mongoose)
2. **Supabase** (PostgreSQL Relational - thông qua `@supabase/supabase-js`)

---

## 1. Cấu hình kiểu dữ liệu Model (Khuyến nghị)
Để TypeScript có thể "nhắc bài" (intellisense) và báo lỗi khi bạn gõ sai, bạn nên định nghĩa Interface hoặc Type cho các bảng (table) hoặc bộ sưu tập (collection) của mình.

```typescript
// Ví dụ Model cho bảng User
export interface IUser {
  id: string;
  email: string;
  full_name: string;
  role: string;
  created_at: Date;
}
```

---

## 2. Tiêm (Inject) Database vào Repository
Trong class Repository (Ví dụ: `UserRepository`), bạn nên truyền `IDatabase` vào Constructor thay vì truyền trực tiếp `SupabaseDatabase` hoặc `MongoDBAtlas`.

```typescript
import { IDatabase } from '../shared/database/database.interface.js';

export class UserRepository {
  // Bằng cách dùng IDatabase, class này không cần quan tâm 
  // bên dưới đang dùng MongoDB hay Supabase.
  constructor(private readonly db: IDatabase) {}
}
```

---

## 3. Các thao tác C.R.U.D Cơ Bản

### A. Thêm mới dữ liệu (`insert`)
Hỗ trợ chèn một đối tượng (Object) hoặc một mảng (Array).

```typescript
  // Thêm 1 User
  async createSingleUser(email: string, fullName: string) {
    const newUser = await this.db.insert<IUser>('users', {
      email: email,
      full_name: fullName,
      role: 'student'
    });
    
    // newUser sẽ mang kiểu dữ liệu là IUser
    return newUser;
  }

  // Thêm nhiều User cùng lúc
  async createMultipleUsers(users: Partial<IUser>[]) {
    const newUsers = await this.db.insert<IUser>('users', users);
    return newUsers; // Trả về mảng IUser[]
  }
```

### B. Tìm kiếm một bản ghi (`findOne`)
Lấy bản ghi đầu tiên khớp với điều kiện. Sẽ trả về `null` nếu không tìm thấy.

```typescript
  async getUserByEmail(email: string) {
    // Điều kiện tìm kiếm (conditions) yêu cầu phải thuộc Partial<IUser>
    // Nếu bạn gõ sai tên cột (VD: email_address), TypeScript sẽ gạch đỏ báo lỗi!
    const user = await this.db.findOne<IUser>('users', { email: email });
    
    if (!user) throw new Error("Không tìm thấy user!");
    return user;
  }
```

### C. Tìm kiếm danh sách kèm Tùy chọn nâng cao (`query`)
Bạn có thể phân trang, sắp xếp và chỉ chọn các cột mong muốn thông qua `IQueryOptions`.

```typescript
  async getActiveStudents(page: number, pageSize: number) {
    const students = await this.db.query<IUser>(
      'users', 
      { role: 'student' }, // Lọc những người có role = student
      {
        // 1. CHỈ LẤY CỘT CẦN THIẾT
        // Supabase: "id, email, full_name" (Hoặc "*, profile(*)" để JOIN bảng)
        // MongoDB: "id email full_name"
        select: 'id, email, full_name', 
        
        // 2. PHÂN TRANG (PAGINATION)
        limit: pageSize,                
        offset: (page - 1) * pageSize,  
        
        // 3. SẮP XẾP (ORDER BY)
        // 'field' bắt buộc phải là 1 key của IUser (như 'created_at')
        orderBy: { field: 'created_at', ascending: false } 
      }
    );
    
    return students; // Trả về mảng IUser[]
  }
```

### D. Cập nhật dữ liệu (`update`)
Cập nhật một hoặc nhiều bản ghi dựa trên điều kiện `conditions`. Hàm trả về danh sách các bản ghi sau khi cập nhật (tùy thuộc vào CSDL).

```typescript
  async updateUserName(userId: string, newName: string) {
    // Tham số 1: Tên bảng/collection
    // Tham số 2: Điều kiện tìm kiếm (VD: id = userId)
    // Tham số 3: Dữ liệu (Payload) cần update
    const updated = await this.db.update<IUser>(
      'users', 
      { id: userId }, 
      { full_name: newName }
    );
    return updated;
  }

  // Update nhiều record cùng lúc
  async deactivateAllStudents() {
    await this.db.update<IUser>(
      'users',
      { role: 'student' },     // Điều kiện: tất cả user là student
      { role: 'inactive_student' } // Dữ liệu mới
    );
  }
```

### E. Xóa dữ liệu (`delete`)
Xóa các bản ghi khớp với điều kiện truyền vào. Trả về `true` nếu thao tác thành công.

```typescript
  async deleteUser(userId: string) {
    const isSuccess = await this.db.delete<IUser>('users', { id: userId });
    
    if (isSuccess) {
      console.log("Xóa thành công!");
    }
  }
```

---

## 4. Tùy biến Nâng cao: Lối thoát hiểm (`getClient`)

Thiết kế Database Interface giải quyết được 90% các nhu cầu truy vấn cơ bản. Tuy nhiên, MongoDB (NoSQL) và Supabase PostgreSQL (Relational) là hai thế giới hoàn toàn khác nhau. Sẽ có lúc bạn cần dùng những tính năng ĐỘC QUYỀN của từng DB (như `Aggregation Pipeline` của MongoDB, hay `RPC / Stored Procedures` của Supabase). 

Lúc này, hãy dùng hàm `getClient<TClient>()`.

### Ví dụ 1: Khi dùng Supabase - Gọi Custom Postgres Function (RPC)
```typescript
  import { SupabaseClient } from '@supabase/supabase-js';

  async callCustomPostgresFunction(userId: string) {
    // Lấy client gốc ra và ép kiểu về SupabaseClient
    const supabase = this.db.getClient<SupabaseClient>();
    
    // Bây giờ bạn có toàn quyền gọi các hàm đặc thù của Supabase
    const { data, error } = await supabase.rpc('my_custom_postgres_function', { user_id: userId });
    
    return data;
  }
```

### Ví dụ 2: Khi dùng MongoDB - Truy vấn Aggregation
```typescript
  import mongoose from 'mongoose';

  async getComplexUserStats() {
    // Lấy client gốc ra (đối với code hiện tại, nó trả về mongoose)
    const mongooseClient = this.db.getClient<typeof mongoose>();
    
    // Gọi thẳng vào collection của mongoose để chạy Aggregation
    const stats = await mongooseClient.connection.db
      .collection('users')
      .aggregate([
         { $match: { role: 'student' } },
         { $group: { _id: "$cohort", count: { $sum: 1 } } }
      ])
      .toArray();

    return stats;
  }
```

---

## Lời khuyên khi sử dụng
1. **Luôn cung cấp Generic Type (`<T>`)**: Khi gọi hàm (VD: `.insert<IUser>`), hãy cung cấp Type rõ ràng. Điều này giúp code bạn **chống được bug sai chính tả tên biến 100%**.
2. **Không lạm dụng `getClient()`**: Chỉ dùng `getClient()` khi thực sự không thể giải quyết bằng các hàm `.query()`, `.insert()`, `.update()` đã có sẵn để giữ cho kiến trúc hệ thống không bị khóa cứng (vendor lock-in) vào 1 loại DB.
