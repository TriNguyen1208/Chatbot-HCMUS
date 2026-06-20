interface RegisterDto {
    email: string;       // Bắt buộc phải có
    password: string;    // Bắt buộc phải có
    age?: number;        // KHÔNG BẮT BUỘC (Có thể có kiểu: number hoặc undefined)
}

// Hợp lệ: Không truyền 'age' cũng không bị lỗi cú pháp TS
const user1: RegisterDto = {
    email: "tri@gmail.com",
    password: "password123"
};

// Cũng hợp lệ: Truyền đầy đủ
const user2: RegisterDto = {
    email: "tri2@gmail.com",
    password: "password123",
    age: 20
};

console.log(user1)