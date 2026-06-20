export const extractEmail = (email: string): string => {
    // Regex này sẽ tìm chuỗi có dạng: [phần chữ][2 chữ số đầu tiên]
    const match = email.match(/^([a-zA-Z]+)(\d{2})/);

    let email_processed = email; // Giá trị mặc định nếu không khớp format

    if (match) {
        const letters = match[1]; // Lấy phần chữ (e.g., "ndtri")
        const firstTwoDigits = match[2]; // Lấy đúng 2 số đầu (e.g., "23")
        
        // Tách lấy phần domain phía sau dấu @ (e.g., "@gmail.com")
        const domain = email.substring(email.indexOf('@')); 
        
        // Gộp lại thành kết quả mong muốn
        email_processed = `${letters}${firstTwoDigits}${domain}`;
    }
    return email_processed
}
export const extractStudentID = (email: string): string | undefined => {
    // BƯỚC 1: Cắt đôi email tại ký tự '@'
    // Ví dụ: ["23127541", "student.edu.vn"]
    const parts = email.split("@");
    const prefix = parts[0]; // Lấy phần đứng trước @ -> "23127541"

    let studentId = undefined;

    // BƯỚC 2: Kiểm tra nếu phần prefix này ĐỒNG THỜI là một chuỗi số hợp lệ
    if (prefix && !isNaN(Number(prefix))) {
        studentId = prefix; // Bạn đã lấy được chuỗi số "23127541"        
    }
    return studentId
}