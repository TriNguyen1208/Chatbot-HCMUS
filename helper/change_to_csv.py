import json
import csv

# Đọc file JSON
with open("input.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# Ghi ra file CSV
with open("output.csv", "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.writer(f)

    # Header
    writer.writerow(["id", "name", "email", "courses"])

    # Dữ liệu
    for item in data:
        writer.writerow([
            item.get("id"),
            item.get("name"),
            item.get("email"),
            "; ".join(item.get("courses", []))  # Nối các course bằng dấu ;
        ])

print("Đã chuyển đổi thành công!")