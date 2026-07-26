# Báo Cáo Phân Tích & Đánh Giá Cấu Trúc Backend (`src/`) - CẬP NHẬT MỚI (KÈM VÍ DỤ CODE)

Tuyệt vời! Mình thấy bạn đã bắt tay vào dọn dẹp dự án rất nhanh. Thư mục `models/` đã được xóa bỏ hoàn toàn, hệ thống giờ đây gọn gàng hơn rất nhiều khi quy chuẩn về một chuẩn duy nhất là `entities/`.

Dưới đây là báo cáo đánh giá cập nhật với những điểm **CÒN LẠI** và **VÍ DỤ CỤ THỂ (CODE CÁCH SỬA)** để bạn có thể tiếp tục refactor:

---

## 1. Vấn Đề Về Cấu Trúc Thư Mục (Folder Structure)

### 1.1. Sai Quy Tắc Đặt Tên File (Số Ít / Số Nhiều)
- **Hiện trạng:** Tên file Service đang để số nhiều: `auth.services.ts`, `user.services.ts`, `auth.strategies.ts`.
- **Đánh giá:** Sai quy tắc (Convention). Tên Class bên trong là số ít (`AuthService`), thì tên file cũng phải là số ít.
- **Gợi ý sửa:** Đổi tên thành:
  - `auth.service.ts`
  - `user.service.ts`
  - `auth.strategy.ts`

### 1.2. Thiếu Layer `infrastructure/` (Hạ Tầng)
- **Hiện trạng:** Thư mục `shared/database/` và `shared/redis.ts` đang nằm trong `shared/`.
- **Đánh giá:** Thư mục `shared` chỉ nên chứa các hàm tiện ích (`utils`), kiểu dữ liệu (`types`) hoặc `middlewares`. Việc kết nối đến server bên ngoài thuộc về **Tầng Hạ Tầng (Infrastructure)**.
- **Gợi ý sửa:**
> **Cấu trúc mong muốn:**
> ```text
> src/
>  ├── infrastructure/
>  │    ├── database/ (chứa supabaseClient, mongoDBAtlas, database.interface)
>  │    └── redis/ (chứa redis.ts)
>  ├── shared/
>  └── modules/
> ```

---

## 2. Vấn Đề Về Logic Code Ở Từng Layer

### 2.1. Vấn đề DTO Mapper (Sự khác biệt giữa Database và API Response)
- **Hiện trạng:** Trong `UserRepository`, bạn đang return thẳng `User` entity ra ngoài Controller. Controller lại `res.json(user)` trả về cho Frontend.
- **Đánh giá rủi ro:** Frontend mong chờ `avatarUrl` nhưng lại nhận được `avatar_url` (do DB đặt tên chuẩn snake_case). Và rủi ro lớn hơn là lộ các dữ liệu nhạy cảm nếu sau này Entity được bổ sung thêm.
- **Gợi ý sửa (VÍ DỤ CODE):** Tại file `user.controller.ts`, bạn nên viết một hàm Mapper nhỏ (hoặc tạo một file `user.mapper.ts`) để "gọt giũa" dữ liệu trước khi ném cho client.

> **Ví dụ trong file `user.controller.ts`:**
> ```typescript
> import { User } from "../entities/user.entity.js";
> import { UserProfile } from "#@/shared/types/index.js"; 
> 
> export class UserController {
>     constructor(private readonly userService: UserService) { }
> 
>     // Hàm Mapper giấu bên trong Controller
>     private toProfileDTO(userEntity: User): UserProfile {
>         return {
>             id: userEntity.id,
>             email: userEntity.email,
>             name: userEntity.name,
>             student_id: userEntity.student_id,
>             phone: userEntity.phone,
>             avatarUrl: userEntity.avatar_url, // Ép từ snake_case sang camelCase
>             createdAt: userEntity.created_at,
>             updatedAt: userEntity.updated_at,
>         };
>     }
> 
>     getMe = async (req: Request, res: Response, next: NextFunction) => {
>         const user_id = req.user!.userID as string;
>         const userEntity = await this.userService.getByID(user_id);
>         
>         if (!userEntity) throw new Error("User not found");
>         
>         // BIẾN ĐỔI ENTITY -> DTO trước khi trả về
>         const safeProfile = this.toProfileDTO(userEntity);
>         return apiResponse.success(res, safeProfile);
>     }
> }
> ```

### 2.2. Các Hàm Tiện Ích (Utils)
- **Hiện trạng:** `student-email.utils.ts` nằm trong User Module.
- **Gợi ý sửa:** Di chuyển file này ra ngoài `src/shared/utils/string.utils.ts` vì nó chỉ là hàm xử lý chuỗi:

---

## 3. Đánh Giá Về Vị Trí Của Các DTOs, Types và Validators

Sau khi kiểm tra 3 file `shared/types/index.ts`, `modules/user/types/index.ts` và `modules/user/user.validator.ts`, mình nhận thấy **bạn đang phân bổ chúng CHƯA ĐÚNG vị trí chuẩn** của Clean Architecture.

### 3.1. Sai lầm 1: Để `UserProfile` trong `shared/types`
- **Phân tích:** `UserProfile` là một DTO (Data Transfer Object) ĐẶC THÙ của module User. Việc nhét nó vào thư mục `shared` là sai nguyên lý. `shared/types` chỉ nên chứa những thứ mang tính toàn cầu (như `JWTPayload`, `TokenPair`, `PaginatedResponse`).
- **Gợi ý sửa:** 
  - Tạo thư mục `src/modules/user/dtos/` (hoặc giữ nguyên `types/`).
  - Chuyển `UserProfile` từ `shared/types/index.ts` sang `modules/user/dtos/user-profile.dto.ts`.

### 3.2. Sai lầm 2: Trùng lặp Entity trong file Types
- **Phân tích:** File `modules/user/types/index.ts` của bạn vẫn còn khai báo `export type KeyStore = { ... }`. 
- **Gợi ý sửa:** Xóa ngay khối code `KeyStore` này đi và import trực tiếp: `import { KeyStore } from "../entities/keystore.entity.js";`

### 3.3. Sai lầm 3: Gom tất cả Validator vào 1 file duy nhất (`user.validator.ts`)
- **Phân tích:** Hiện tại bạn dùng Zod rất hay (infer ra Type từ Schema). Nhưng việc nhét mọi thứ (`GoogleLoginSchema`, `UpdateProfileSchema`, `GetByIDParams`) vào 1 file sẽ làm file này phình to rất nhanh.
- **Gợi ý sửa (Best Practice):** Zod Schema bản chất chính là DTO (đầu vào). Hãy chuyển chúng vào thư mục `dtos/` và chia nhỏ theo từng chức năng.

> **Cấu trúc DTO & Validator mong muốn:**
> ```text
> src/modules/user/
>  ├── dtos/
>  │    ├── google-login.dto.ts     (Chứa Zod GoogleLoginSchema + Type)
>  │    ├── update-profile.dto.ts   (Chứa Zod UpdateProfileSchema + Type)
>  │    ├── get-by-id.dto.ts        (Chứa Zod GetByIDParams)
>  │    └── user-profile.dto.ts     (Chứa type UserProfile trả về cho Frontend)
>  ├── entities/
> ```

> **Ví dụ nội dung file `src/modules/user/dtos/update-profile.dto.ts`:**
> ```typescript
> import { z } from "zod";
> 
> export const UpdateProfileSchema = z.object({
>     body: z.object({
>         name: z.string().optional(),
>         phone: z.string().optional(),
>         avatarUrl: z.url("URL không hợp lệ").optional(),
>     }).strict(),
> });
> 
> // Infer type ngay trong file DTO
> export type UpdateProfileDto = z.infer<typeof UpdateProfileSchema>;
> ```

Nhờ cách tổ chức `dtos/` này, code của bạn sẽ cực kỳ module hóa, dễ tìm kiếm và mở rộng y hệt như các framework chuyên nghiệp.

---

## 4. Tổng Kết Các Bước Cần Làm Tiếp Theo
1. Đổi tên các file Services thành số ít.
2. Tạo thư mục `src/infrastructure` và chuyển cấu hình Database + Redis vào đó.
3. Chuyển `UserProfile` về lại module User.
4. (Tùy chọn nâng cao) Đập bỏ file `user.validator.ts` và chia nhỏ thành các file `.dto.ts` riêng biệt bỏ vào thư mục `dtos/`.
