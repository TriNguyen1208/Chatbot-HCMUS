# Đánh giá tổng thể kiến trúc và Codebase Backend

Dưới đây là một số vấn đề (issues) và điểm chưa ổn trong cấu trúc thư mục, code và logic của thư mục `backend` dựa trên việc xem xét mã nguồn hiện tại:

## 1. Thiếu `asyncHandler` ở một số route (Lỗi nghiêm trọng)
- **Vị trí**: `src/modules/auth/routes/auth.route.ts` (Dòng 37)
- **Vấn đề**: Route `/refresh-token` đang gọi trực tiếp `authController.refreshToken` mà không được bọc qua `asyncHandler`.
  ```typescript
  router.post("/refresh-token", AuthMiddleware.verifyRefreshToken, authController.refreshToken)
  ```
- **Hậu quả**: Nếu quá trình xử lý `refreshToken` (vốn là async) ném ra một ngoại lệ (exception), Express sẽ không thể tự động bắt được lỗi này, dẫn đến `Unhandled Promise Rejection` có thể làm crash toàn bộ server (hoặc treo request).
- **Khắc phục**: Bọc hàm controller lại bằng `asyncHandler(authController.refreshToken)`.
  ```typescript
  // Trước
  router.post("/refresh-token", AuthMiddleware.verifyRefreshToken, authController.refreshToken)

  // Sau
  router.post("/refresh-token", AuthMiddleware.verifyRefreshToken, asyncHandler(authController.refreshToken))
  ```

## 2. Dependency Injection (DI) thủ công và rườm rà
- **Vị trí**: Các file route như `auth.route.ts`, `user.route.ts` và cả `user.facade.ts`.
- **Vấn đề**: Hệ thống đang tạo ra các instance (khởi tạo bằng từ khóa `new`) một cách thủ công ngay trong file định tuyến (route) hoặc tự khởi tạo các repository bên trong constructor của class (ví dụ: `UserFacade` tự khởi tạo `UserRepository`).
- **Hậu quả**:
  - Code bị **tight-coupling** (phụ thuộc chặt chẽ), rất khó để viết Unit Test vì không thể dễ dàng mock các service/repository.
  - Sẽ vô tình tạo ra nhiều instance trùng lặp của cùng một repository (ví dụ `UserRepository` được `new` trong `user.route.ts` và lại được `new` thêm lần nữa trong `UserFacade`).
- **Khắc phục**: Nên sử dụng các thư viện hỗ trợ Dependency Injection như `tsyringe`, `inversify` hoặc tự xây dựng một `AppContainer` ở cấp độ root để quản lý vòng đời (singleton) của các class này và truyền vào constructor.
  
  **Gợi ý cấu trúc `AppContainer` (Manual DI):**
  ```typescript
  // src/container.ts
  import { supabaseDB } from "#@/infrastructure/database/supabaseClient.js";
  import { UserRepository } from "#@/modules/user/repositories/user.repository.js";
  import { UserService } from "#@/modules/user/services/user.service.js";
  import { UserController } from "#@/modules/user/controllers/user.controller.js";

  class AppContainer {
      public userRepository = new UserRepository(supabaseDB);
      public userService = new UserService(this.userRepository);
      public userController = new UserController(this.userService);
      // Khởi tạo các module khác tương tự...
  }
  export const container = new AppContainer();
  ```
  Và trong route `user.route.ts`:
  ```typescript
  import { container } from "#@/container.js";
  
  const router = Router();
  router.get("/me", AuthMiddleware.verifyAccessToken, asyncHandler(container.userController.getMe));
  // ...
  ```

## 3. Lỗi tiềm ẩn ở Middleware xử lý lỗi (Error Handler)
- **Vị trí**: `src/shared/middlewares/error.middleware.ts`
- **Vấn đề**: Khi bắt được một lỗi có HTTP Code = 500, hệ thống trả về thông báo "Internal Server Error" cho client (điều này tốt để bảo mật), **nhưng lại không hề log stack trace (chi tiết lỗi) ra console.**
- **Hậu quả**: Khi code bị lỗi trên môi trường production hay development, dev sẽ hoàn toàn "mù tịt" không biết lỗi xuất phát từ dòng nào hay file nào.
- **Khắc phục**: Cần thêm `console.error(err)` hoặc sử dụng một logger (như `winston`, `pino`) để ghi nhận log ở phía server trước khi response về cho client.
  ```typescript
  export const errorHandler = (
      err: AppError,
      _req: Request,
      res: Response, 
      next: NextFunction
  ): void => {
      const statusCode = err.statusCode || 500
      const code = err.code || "Internal_Error"

      // Gợi ý code: Log lỗi ra console để debug
      console.error(`[Error] ${code}:`, err);

      res.status(statusCode).json({
          code,
          message: statusCode === 500 ? "Internal Server Error" : err.message
      })
  }
  ```

## 4. Hardcode cấu hình và rủi ro bảo mật từ `dotenv` fallback
- **Vị trí**: `src/app.ts` và `src/config/config.ts`
- **Vấn đề**:
  - `cors({ origin: "http://localhost:3000" })` đang bị fix cứng URL, khi đưa lên server production sẽ phải sửa code.
  - Các cấu hình nhạy cảm đang được cấp giá trị mặc định thiếu an toàn, ví dụ: 
    `accessSecret: process.env.JWT_ACCESS_SECRET || "your_access_secret"`
- **Hậu quả**: Lộ lọt mã bí mật (Secret) nếu file `.env` không được config chính xác ở production, hệ thống sẽ tự động dùng "your_access_secret" khiến hacker dễ dàng giả mạo JWT Token.
- **Khắc phục**: Lấy origin của CORS từ biến môi trường. Với các biến bắt buộc (Secret), nếu không tồn tại trong `process.env` thì phải quăng lỗi (Throw Error) và dừng server ngay lập tức để cảnh báo.
  ```typescript
  // Trong src/config/config.ts
  const accessSecret = process.env.JWT_ACCESS_SECRET;
  if (!accessSecret) {
      throw new Error("Missing JWT_ACCESS_SECRET in environment variables");
  }

  this.corsOrigin = process.env.CORS_ORIGIN || "http://localhost:3000";
  this.jwt = {
      accessSecret: accessSecret,
      // ...
  }
  
  // Trong src/app.ts
  app.use(cors({
      origin: config.corsOrigin,
      credentials: true
  }));
  ```

## 5. Quản lý kết nối WebSocket bị hạn chế (Single Device)
- **Vị trí**: `src/infrastructure/websocket/socket-manager.ts`
- **Vấn đề**: Biến lưu trữ người dùng đang online sử dụng `Map<string, string>()` (map từ `userId` sang 1 `socketId` duy nhất).
- **Hậu quả**: Nếu một người dùng đăng nhập bằng **nhiều thiết bị cùng lúc** (ví dụ: vừa dùng Laptop, vừa dùng Điện thoại), thiết bị kết nối sau sẽ ghi đè lên `socketId` của thiết bị trước. Khi hệ thống muốn `emitToUser`, chỉ thiết bị cuối cùng nhận được thông báo.
- **Khắc phục**: Nên thay đổi cấu trúc dữ liệu thành `Map<string, Set<string>>` để 1 `userId` có thể ánh xạ đến nhiều `socketId`.
  ```typescript
  // 1. Sửa biến
  private userSocketMap = new Map<string, Set<string>>();

  // 2. Trong sự kiện connection
  if (userId) {
      if (!this.userSocketMap.has(userId)) {
          this.userSocketMap.set(userId, new Set());
      }
      this.userSocketMap.get(userId)!.add(socket.id);
  }

  // 3. Sự kiện disconnect
  socket.on("disconnect", () => {
      if (userId && this.userSocketMap.has(userId)) {
          this.userSocketMap.get(userId)!.delete(socket.id);
          if (this.userSocketMap.get(userId)!.size === 0) {
              this.userSocketMap.delete(userId);
          }
      }
  });

  // 4. Khi emitToUser
  public emitToUser(userId: string, event: string, data: any) {
      const socketIds = this.userSocketMap.get(userId);
      if (socketIds) {
          socketIds.forEach(socketId => {
              this.io.to(socketId).emit(event, data);
          });
      }
  }
  ```

## 6. Lỗi tiềm ẩn khi khởi động server
- **Vị trí**: `src/index.ts`
- **Vấn đề**: Hàm `start()` được gọi thông qua Top-level await (`await start()`), nếu `mongoDB.connect()` hoặc `redisClient.connect()` bị lỗi kết nối do sai URL hoặc sập mạng, promise sẽ bị reject.
- **Hậu quả**: Ứng dụng Node.js sẽ kết thúc với lỗi mà đôi khi không có log rõ ràng, hoặc bị crash âm thầm.
- **Khắc phục**: Nên bọc bằng block `try...catch` hoặc `.catch(err => { console.error(err); process.exit(1); })` để đảm bảo lỗi khởi tạo Database luôn được hiển thị ra màn hình và ngắt chương trình một cách dứt khoát.
  ```typescript
  // Trong src/index.ts
  start().catch(err => {
      console.error("Failed to start server:", err);
      process.exit(1);
  });
  ```

## 7. Truyền dữ liệu giữa các layer (Sử dụng DTO - Data Transfer Object)
- **Vấn đề**: Việc truyền dữ liệu giữa các layer (ví dụ: từ Route -> Controller -> Service) có thể đang phải truyền quá nhiều tham số rời rạc (ví dụ: `createUser(email, password, name, age)`) hoặc dùng các Object không có kiểu dữ liệu rõ ràng (`any`), làm code khó đọc, khó mở rộng và mất khả năng gợi ý code (auto-complete) của TypeScript.
- **Giải pháp**: Nhóm các dữ liệu liên quan lại thành một object duy nhất và định nghĩa kiểu dữ liệu cụ thể cho nó. Khái niệm này gọi là **DTO (Data Transfer Object)**.
- **Vị trí file**: Nên tạo thư mục `dtos` (hoặc `types`, `interfaces`) bên trong thư mục của mỗi module.
  - Ví dụ: `src/modules/user/dtos/create-user.dto.ts`
- **Cách áp dụng**:
  
  **Bước 1: Định nghĩa Type/Interface cho DTO**
  ```typescript
  // src/modules/user/dtos/create-user.dto.ts
  export interface CreateUserDTO {
      email: string;
      fullName: string;
      avatarUrl?: string; // Dấu ? nghĩa là không bắt buộc (optional)
  }
  ```

  **Bước 2: Sử dụng DTO ở Controller**
  ```typescript
  // src/modules/user/controllers/user.controller.ts
  import { CreateUserDTO } from "../dtos/create-user.dto.js";

  export class UserController {
      public createUser = async (req: Request, res: Response) => {
          // Ép kiểu req.body sang DTO để đảm bảo an toàn kiểu dữ liệu
          const userData: CreateUserDTO = req.body; 
          
          // Truyền nguyên object "userData" xuống cho Service xử lý
          const newUser = await this.userService.createUser(userData);
          res.json(newUser);
      }
  }
  ```

  **Bước 3: Tiếp nhận DTO ở Service**
  ```typescript
  // src/modules/user/services/user.service.ts
  import { CreateUserDTO } from "../dtos/create-user.dto.js";

  export class UserService {
      // Hàm nhận vào 1 object duy nhất thuộc kiểu CreateUserDTO
      public async createUser(data: CreateUserDTO) {
          // TypeScript sẽ tự động gợi ý data.email, data.fullName
          return await this.userRepository.insert(data);
      }
  }
  ```
- **Lợi ích mang lại**:
  - **Code gọn gàng hơn**: Thay vì truyền 5, 10 tham số rời rạc, bạn chỉ cần truyền đúng 1 object.
  - **Dễ mở rộng**: Nếu sau này muốn thêm thuộc tính `phoneNumber` để tạo user, bạn chỉ việc thêm `phoneNumber: string` vào file interface `CreateUserDTO`. Các hàm từ Controller xuống Service giữ nguyên (không cần sửa chữ ký/thứ tự tham số).
  - **An toàn kiểu dữ liệu (Type-safe)**: Hạn chế tối đa lỗi truyền nhầm vị trí tham số, tận dụng sức mạnh gợi ý code của TypeScript.

### 7.1 Xử lý khi dữ liệu truyền ở các layer là khác nhau
Trong thực tế, dữ liệu client gửi lên (ở Controller) thường không giống hoàn toàn với dữ liệu mà Service cần, hoặc dữ liệu Service ghi xuống DB (ở Repository) cũng khác.
**Giải pháp**: Tạo ra nhiều DTO khác nhau cho từng mục đích (hoặc tận dụng tính năng `Omit`, `Pick` của TypeScript để kế thừa).

**Ví dụ thực tế**:
1. **Request DTO** (Từ Client -> Controller): Chứa những dữ liệu thô người dùng nhập vào.
```typescript
// dtos/create-user.request.dto.ts
export interface CreateUserRequestDTO {
    email: string;
    passwordRaw: string; // Password chưa mã hóa
    fullName: string;
}
```

2. **Service DTO** (Từ Controller -> Service): Controller có thể phải thêm thắt dữ liệu (như `ipAddress`, `deviceId`) trước khi đẩy xuống Service.
```typescript
// dtos/create-user.service.dto.ts
export interface CreateUserServiceDTO {
    email: string;
    passwordRaw: string;
    fullName: string;
    ipAddress: string; // Thêm IP lấy từ req.ip
}
```
*Cách Controller gọi Service:*
```typescript
const serviceData: CreateUserServiceDTO = {
    ...req.body,
    ipAddress: req.ip
};
await this.userService.createUser(serviceData);
```

3. **Repository DTO / Entity** (Từ Service -> Database): Service xử lý logic (ví dụ: mã hóa password, tạo ID) rồi mới truyền xuống Repository để lưu.
```typescript
// dtos/create-user.db.dto.ts (hoặc định nghĩa thẳng trong Entity)
export interface CreateUserDbDTO {
    id: string; // Đã sinh UUID
    email: string;
    passwordHash: string; // Đã băm (hash)
    fullName: string;
    createdAt: Date;
}
```
*Cách Service gọi Repository:*
```typescript
const dbData: CreateUserDbDTO = {
    id: generateId(),
    email: data.email,
    passwordHash: hash(data.passwordRaw),
    fullName: data.fullName,
    createdAt: new Date()
};
return await this.userRepository.insert(dbData);
```

**Mẹo viết code gọn**: Thay vì tạo quá nhiều file, bạn có thể gom chung vào 1 file `user.dto.ts` hoặc dùng các Utility Types (`Omit`, `Pick`, `Partial`) của TypeScript để tái sử dụng mà không phải lặp lại code:
```typescript
// Tái sử dụng type bằng Utility Types
export interface BaseUser {
    email: string;
    fullName: string;
}

export interface CreateUserRequestDTO extends BaseUser {
    passwordRaw: string;
}

export interface CreateUserDbDTO extends BaseUser {
    id: string;
    passwordHash: string;
}
```

### 7.2 Code mẫu cần refactor bằng DTO trong dự án của bạn (Module Auth)
Dưới đây là một chỗ cụ thể trong file `src/modules/auth/services/keystore.service.ts` đang dùng Inline Object dài dòng và cần được đưa ra thành DTO.

**Đoạn code gốc hiện tại (Chưa tốt)**:
Hàm `saveRefreshToken` và `refreshToken` đang định nghĩa trực tiếp object khổng lồ không có tên kiểu (Inline Type):
```typescript
// Trong file: src/modules/auth/services/keystore.service.ts
async saveRefreshToken({
    userID,
    rawRefreshToken,
    device_info,
    // ...
}: {
    userID: string,
    rawRefreshToken: string,
    device_info?: {
        user_agent?: string | undefined,
        ip?: string | undefined
    } | undefined,
    family_id?: string | undefined,
    parent_id?: string | null,
    expires_at?: Date
}): Promise<void> {
    // ... logic code
}
```

**Đoạn code sau khi Refactor sử dụng DTO (Nên làm)**:
1. Tạo một file `dto/keystore.dto.ts` (ví dụ `src/modules/auth/dto/keystore.dto.ts`):
```typescript
import type { JWTPayload } from "#@/shared/types/index.js";

export interface DeviceInfoDTO {
    user_agent?: string;
    ip?: string;
}

export interface SaveRefreshTokenDTO {
    userID: string;
    rawRefreshToken: string;
    device_info?: DeviceInfoDTO;
    family_id?: string;
    parent_id?: string | null;
    expires_at?: Date;
}

export interface RefreshTokenDTO {
    rawRefreshToken: string;
    deviceInfo?: DeviceInfoDTO;
    user: JWTPayload;
}
```

2. Sửa lại code trong `keystore.service.ts` để tái sử dụng DTO, nhìn sẽ ngắn gọn và chuyên nghiệp hơn rất nhiều:
```typescript
import { SaveRefreshTokenDTO, RefreshTokenDTO } from "../dto/keystore.dto.js";

export class KeystoreService {
    // ...

    async saveRefreshToken(data: SaveRefreshTokenDTO): Promise<void> {
        const hashRefreshToken = sha256(data.rawRefreshToken);
        let expires_at = data.expires_at;
        
        if (!expires_at) {
            expires_at = new Date(Date.now() + parseDurationMs(config.jwt.refreshExpires as string));
        }

        const payloadDatabase: KeyStore = {
            user_id: data.userID,
            refresh_token_hash: hashRefreshToken,
            family_id: data.family_id ?? uuidv4(),
            parent_id: data.parent_id ?? null,
            is_used: false,
            device_info: data.device_info,
            expires_at: expires_at
        };
        
        await this.keystoreRepo.create(payloadDatabase);
    }

    async refreshToken(data: RefreshTokenDTO) {
        const hashRefreshToken = sha256(data.rawRefreshToken);
        const keyStore = await this.keystoreRepo.findByHash(hashRefreshToken);
        // ... gọi các thuộc tính thông qua data.user, data.deviceInfo
    }
}
```

3. Ở `auth.controller.ts` khi gọi service, bạn vẫn truyền tham số như bình thường (Object destructuring), nhưng TypeScript sẽ tự động kiểm tra chặt chẽ dựa trên DTO mới:
```typescript
await this.keystoreService.saveRefreshToken({
    userID: result.user.id,
    rawRefreshToken: result.tokens.refreshToken,
    device_info: {
        user_agent: req.headers["user-agent"] as string,
        ip: req.ip
    }
}); // Báo lỗi ngay lập tức nếu code thiếu thuộc tính bắt buộc của SaveRefreshTokenDTO.
```

## 8. Các lỗi nghiêm trọng (Bugs) cần khắc phục trong module `auth`

Sau khi rà soát thư mục `src/modules/auth`, có 2 lỗi logic lớn ảnh hưởng trực tiếp đến trải nghiệm người dùng (UX) và bảo mật cần được sửa ngay:

### 8.1. Bug: Refresh Token làm đăng xuất tất cả các thiết bị khác
- **Vị trí**: 
  - `keystore.service.ts` (dòng 85): `await this.keystoreRepo.markUsed(keyStore.user_id as any);`
  - `keystore.repository.ts` (dòng 27): `this.db.update<KeyStore>("keystores", { user_id: user_id }, { is_used: true })`
- **Vấn đề**: Khi một người dùng (user) gọi API `/refresh-token` thành công, hệ thống cần đánh dấu refresh token *vừa được sử dụng* là `is_used = true` để chống dùng lại. Tuy nhiên, code hiện tại lại update tất cả các document có `user_id` của user đó.
- **Hậu quả**: Nếu user đăng nhập trên 2 thiết bị (điện thoại và laptop). Khi điện thoại hết hạn access token và tự động gọi `refresh-token`, hệ thống sẽ vô tình đánh dấu `is_used = true` cho cả token của laptop. Lần tiếp theo laptop dùng token của nó để refresh, hệ thống sẽ báo *"Phát hiện bất thường, vui lòng đăng nhập lại"* và xoá sạch phiên đăng nhập.
- **Cách khắc phục**: Chỉ đánh dấu token hiện tại. Thay vì gọi `markUsed`, hãy gọi hàm đã có sẵn là `markUsedByHash`.
  ```typescript
  // Trong keystore.service.ts sửa thành:
  await this.keystoreRepo.markUsedByHash(hashRefreshToken);
  ```

### 8.2. Bug: Không thể Logout nếu Access Token đã hết hạn
- **Vị trí**: `src/modules/auth/routes/auth.route.ts` (dòng 15, 17)
  ```typescript
  router.post("/logout", AuthMiddleware.verifyAccessToken, asyncHandler(...))
  ```
- **Vấn đề**: API `/logout` đang bị chặn bởi `verifyAccessToken`.
- **Hậu quả**: Nếu access token của user đã hết hạn (expired), khi user ấn nút "Đăng xuất" trên giao diện, request gửi lên sẽ bị middleware đá văng ra ngay với lỗi `401 Unauthorized` (Token expired). Kết quả là code không thể đi vào `AuthController.logout`, refresh token trong Database không bị thu hồi, và cookie không được dọn dẹp (clear). User sẽ bị "kẹt", không thể đăng xuất trừ khi app lén gọi refresh token trước khi logout (rất thừa thãi).
- **Cách khắc phục**: API `/logout` nên sử dụng `AuthMiddleware.verifyRefreshToken` thay vì access token, vì mục đích chính của logout là vô hiệu hóa refresh token.
  ```typescript
  // Trong auth.route.ts sửa thành:
  router.post("/logout", AuthMiddleware.verifyRefreshToken, asyncHandler(authContainer.authController.logout))
  ```

### 7.3 Code mẫu cần refactor bằng DTO trong dự án của bạn (Module User)
Tương tự như `Auth`, trong thư mục `src/modules/user/repositories/user.repository.ts`, hàm `create` cũng đang nhận vào một Inline Object dài dòng.

**Đoạn code gốc hiện tại (Chưa tốt)**:
```typescript
// Trong file: src/modules/user/repositories/user.repository.ts
async create({ email, name, avatar_url, student_id }: { email: string; name: string; avatar_url: string; student_id?: string | undefined }): Promise<User> {
    const row = await this.db.insert<User>('users', { email, name, avatar_url, student_id })
    return row as User
}
```

**Đoạn code sau khi Refactor sử dụng DTO**:
Bạn chỉ cần khai báo DTO và truyền thẳng object vào Database (vì cấu trúc đã khớp với các field của database):
```typescript
// Trong file: src/modules/user/dto/user.dto.ts
export interface CreateUserDTO {
    email: string;
    name: string;
    avatar_url?: string;
    student_id?: string;
}

// Trong file: src/modules/user/repositories/user.repository.ts
async create(data: CreateUserDTO): Promise<User> {
    // Truyền thẳng object "data" vào hàm insert của Database, code rất gọn
    const row = await this.db.insert<User>('users', data);
    return row as User;
}
```
Nhờ DTO, nếu sau này bảng User có thêm trường `phone`, bạn chỉ việc cập nhật interface `CreateUserDTO` mà không cần phải đi dò để thay đổi chữ ký (signature) của hàm `create`.

## 9. Các vấn đề và lỗi (Bugs) cần khắc phục trong module `user`

Sau khi rà soát thư mục `src/modules/user`, mình tìm thấy một số vấn đề logic cần xử lý:

### 9.1. Lỗi logic (Bug) trong việc chuẩn hoá định dạng Email
- **Vị trí**: `src/modules/user/repositories/user.repository.ts`
- **Vấn đề**:
  - Trong hàm `findByEmail`: Code đang tìm kiếm user bằng cách hạ email về chữ thường (`email.toLowerCase()`).
  - Tuy nhiên, trong hàm `create`: Code lưu trữ `email` giữ nguyên định dạng chữ viết hoa/thường ban đầu.
- **Hậu quả**: Nếu user đăng nhập bằng Google với email "NguyenVanA@gmail.com", hàm `create` sẽ lưu y nguyên chữ hoa đó vào DB. Nhưng các hàm gọi đến `findByEmail("NguyenVanA@gmail.com")` sẽ bị rỗng (null), vì nó đã bị chuyển thành `nguyenvana@gmail.com` để query, dẫn tới việc DB không tìm thấy kết quả khớp hoàn toàn (exact match).
- **Cách khắc phục**: Phải đồng nhất quy tắc hạ lowercase. Nên ép toàn bộ email thành chữ thường (lowercase) ở hàm `create` trước khi lưu vào DB.
  ```typescript
  async create(data: CreateUserDTO): Promise<User> {
      data.email = data.email.toLowerCase(); // Đảm bảo email luôn ở dạng lowercase
      const row = await this.db.insert<User>('users', data);
      return row as User;
  }
  ```

### 9.2. Code lặp (Duplicated API Endpoint)
- **Vị trí**: 
  - `src/modules/user/routes/user.route.ts` (Dòng 10): `router.get("/me", ...)`
  - `src/modules/auth/routes/auth.route.ts` (Dòng 19): `router.get("/me", ...)`
- **Vấn đề**: API lấy thông tin người dùng `GET /me` đang được định nghĩa ở tận 2 nơi (cả Auth Route và User Route) và trỏ đến 2 Controller khác nhau (`authController.getMe` và `userController.getMe`).
- **Cách khắc phục**:
  Việc lấy thông tin cá nhân Profile (của User) nên đặt ở module `user`. Bạn nên **xoá** route `/me` trong thư mục auth (`auth.route.ts`), và xoá luôn hàm `getMe` bên `auth.controller.ts` để dọn dẹp code rác, đảm bảo tính duy nhất (Single Source of Truth). Dùng hàm `getMe` trong `user.controller.ts` là chuẩn xác nhất.
