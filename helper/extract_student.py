import pandas as pd
from unidecode import unidecode
import json
from collections import defaultdict

# Thêm engine='openpyxl' để ép pandas dùng đúng thư viện đọc file .xlsx
file_path = "/Users/ductri0981/Documents/Website/Chatbot-HCMUS/helper/LichThi-ChiTiet-CK-HK1_23-24-DeAn-CNTT.xlsx"
file_paths = [
    "LichThi-ChiTiet-CK-HK1_23-24-DeAn-CNTT.xlsx",
    "LichThi-ChiTiet-CK-HK1_24-251_DeAn_CNTT-1.xlsx",
    "LichThi-ChiTiet-CK-HK1_25-26-DA-CNTT-1.xlsx",
    "LichThi-ChiTiet-CK-HK2_23-24-DeAn-CNTT.xlsx",
    "LichThi-ChiTiet-CK-HK2_24-25-DA-CNTT.xlsx",
    "LichThi-ChiTiet-CK-HK2_25-26-DA-CNTT-1.xlsx",
    "LichThi-ChiTiet-CK-HK3_23-243-DeAn-CNTT.xlsx",
    "LichThi-ChiTiet-CK-HK3_24-25-KhoaCNTT.xlsx",
]
try:
    students = set()
    for file_path in file_paths:
        df = pd.read_excel(file_path, engine='openpyxl')
        for row in df.itertuples(index=False):
            student_name = row.hoten
            student_id = row.masv
            student_year = str(student_id)[:2]

            if str(student_id)[2:5] == "127":
                student_program = "clc"
            elif str(student_id)[2:5] == "126":
                student_program = "vp"
            elif str(student_id)[2:5] == "125":
                student_program = "apcs"
            else:
                student_program = "unknown"

            student_last_name = student_name.split(" ")[-1]
            name_array = student_name.split(" ")
            email_prefix = "".join([name[0] for idx, name in enumerate(name_array) if idx != len(name_array) - 1])
            # Cộng thêm tên chính vào đuôi
            email = email_prefix + student_last_name + str(student_id)[:2] + "@" + student_program + ".fitus.edu.vn"
            safe_email = unidecode(email).lower()

            if student_program == "unknown":
                students.add((row.masv, row.hoten, None))
            else:
                students.add((row.masv, row.hoten, safe_email))
    
    student_json = []
    for student in students:
        student_json.append({
            "student_id": student[0],
            "full_name": student[1],
            "email": student[2]
        })
    student_json.sort(key=lambda x: x["student_id"])
    
    # In thử kết quả để kiểm tra
    with open("student.json", "w", encoding="utf-8") as f:
        json.dump(student_json, f, ensure_ascii=False, indent=4)

    
except FileNotFoundError:
    print("Lỗi: Không tìm thấy file! Bạn hãy kiểm tra lại đường dẫn xem đã chính xác chưa nhé.")