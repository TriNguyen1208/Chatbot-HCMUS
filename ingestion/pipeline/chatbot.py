import requests
from qdrant_client import QdrantClient
from google import genai
from dotenv import load_dotenv
import os

from ingestion.pipeline.database.db import QdrantVectorDB

db = QdrantVectorDB()

# Run
# user_query = "Khi nào thì sinh viên được miễn các học phần tiếng Anh?"
user_query = "Mục tiêu chung của chương trình đào tạo chính quy?"

# -- 1. search chunks for LLM context
search_results = db.semantic_search("hcmus_documents", user_query, 0.4, 10)
print(len(search_results))

context = "\n---\n".join([item.payload["content"] for item in search_results])

# -- 2. 
prompt = f"""Bạn là một trợ lý AI chỉ trả lời dựa trên tài liệu được cung cấp.

[QUY TẮC BẮT BUỘC]:
1. CHỈ sử dụng thông tin trong phần [DỮ LIỆU CONTEXT] dưới đây để trả lời.
2. Nếu trong [DỮ LIỆU CONTEXT] KHÔNG có thông tin để trả lời, bạn BẮT BUỘC phải nói: "Tôi không tìm thấy thông tin này trong tài liệu được cung cấp."
3. TUYỆT ĐỐI KHÔNG tự bịa ra câu trả lời hoặc dùng kiến thức bên ngoài [DỮ LIỆU CONTEXT].
4. Trả lời ngắn gọn, đúng trọng tâm.

[DỮ LIỆU CONTEXT]:
{context}

[CÂU HỎI]:
{user_query}"""

client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))
response = client.models.generate_content(
    model="gemini-2.5-flash",
    contents=prompt
)
print(response.text)