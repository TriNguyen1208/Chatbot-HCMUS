from google import genai
import os
from typing import List
from pydantic import BaseModel

from pipeline.database.db import QdrantVectorDB

db = QdrantVectorDB()
client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))

class RAGResponse(BaseModel):
    answer: str
    source_indices: List[int]

while(True):
    user_query = input("\n> Câu hỏi: ")
    if user_query.strip().lower() in ['quit', 'exit']:
        break

    search_results = db.hybrid_search_with_rerank("TempCollection", user_query, top_k=3)
    if (search_results == []):
        print("> Trả lời: Không đủ dữ kiện trả lời!")
        continue

    context_parts = []
    for i, item in enumerate(search_results):
        context_parts.append(f"[{i}] {item.payload['content']}")
    context = "\n\n".join(context_parts)

    prompt = f"""Bạn là một trợ lý AI tra cứu thông tin. Nhiệm vụ của bạn là trả lời câu hỏi dựa trên các tài liệu được cung cấp.
        [QUY TẮC BẮT BUỘC]:
        1. CHỈ sử dụng thông tin trong phần [DỮ LIỆU CONTEXT] để trả lời.
        2. Nếu [DỮ LIỆU CONTEXT] không có thông tin hoặc không đủ dữ kiện, BẮT BUỘC phải đặt `answer` là "Không đủ dữ kiện!" và `source_indices` là mảng rỗng [].
        3. TUYỆT ĐỐI KHÔNG tự bịa ra câu trả lời hoặc dùng kiến thức bên ngoài.
        4. Trả về đúng các số index (ví dụ: [0], [0, 2]) của những đoạn văn bản mà bạn đã thực sự dùng để trích xuất câu trả lời vào trường `source_indices`.

        [DỮ LIỆU CONTEXT]:
        {context}

        [CÂU HỎI]:
        {user_query}
    """
        
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
        config={
            "response_mime_type": "application/json",
            "response_schema": RAGResponse,
            "temperature": 0.0
        }
    )

    result = response.parsed
    
    print(f"> Trả lời: {result.answer}")
    if result.source_indices:
        print(" # Nguồn tham khảo:")
        for i, index in enumerate(result.source_indices):
            print(f"Nguồn {i + 1}.\n", search_results[index].payload["content"], '\n-----------------')