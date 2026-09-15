from google import genai
import os
from typing import List, Dict, Any
from pydantic import BaseModel

from pipeline.database.db import QdrantVectorDB

COLLECTION_NAME = "HCMUS-DATA"
db = QdrantVectorDB()

db.client.create_payload_index(
    collection_name=COLLECTION_NAME,
    field_name="document_id",
    field_schema="keyword"
)

db.client.create_payload_index(
    collection_name=COLLECTION_NAME,
    field_name="chunk_index",
    field_schema="integer"
)

client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))

class RAGResponse(BaseModel):
    answer: str
    source_indices: List[int]

def merge_adjacent_chunks(search_results: List[Dict[Any, Any]], max_chunk_dist: int = 1) -> List[Dict[str, Any]]:
    """
    Merge consecutive chunks (belong to the same document and having consecutive chunk's ids).
    
    Args:
        search_results: list of chunks.
        max_chunk_dist: maximum chunk_id distance between 2 chunks to be merged.
    """
    if not search_results:
        return []

    # Group chunks by document_id
    grouped_by_doc: Dict[str, List[Dict]] = {}
    for item in search_results:
        doc_id = item.get("document_id")
        if doc_id not in grouped_by_doc:
            grouped_by_doc[doc_id] = []
        grouped_by_doc[doc_id].append(item)

    merged_results = []

    # Merge chunks in each document
    for doc_id, chunks in grouped_by_doc.items():
        chunks.sort(key=lambda x: x["chunk_index"])

        current_block = chunks[0].copy()
        current_texts = [chunks[0]["content"]]

        for next_chunk in chunks[1:]:
            prev_index = current_block["chunk_index"]
            curr_index = next_chunk["chunk_index"]

            if curr_index - prev_index == max_chunk_dist:
                current_texts.append(next_chunk["content"])
                current_block["chunk_index"] = curr_index
                # current_block["score"] = max(current_block["score"], next_chunk["score"])
            else:
                current_block["content"] = "\n\n".join(current_texts)
                merged_results.append(current_block)
                
                current_block = next_chunk.copy()
                current_texts = [next_chunk["content"]]

        current_block["content"] = "\n\n".join(current_texts)
        merged_results.append(current_block)

    # merged_results.sort(key=lambda x: x["score"], reverse=True)
    return merged_results

if __name__ == "__main__":
    while(True):
        user_query = input("\n> Câu hỏi: ")
        if user_query.strip().lower() in ['quit', 'exit']:
            break

        raw_results = db.hybrid_search_with_rerank(COLLECTION_NAME, user_query, prefetch_limit=50, fusion_limit=30, top_k=15)
        raw_results = [item.payload for item in raw_results]
        if (raw_results == []):
            print("> Trả lời: Không đủ dữ kiện trả lời!")
            continue
        context_blocks = db.build_context_blocks(COLLECTION_NAME, raw_results, extra_chunks=2)

        context_parts = []
        for i, item in enumerate(context_blocks):
            context_parts.append(f"[{i}] {item['content']}")
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
                print(f"Nguồn {i + 1}.\n", context_blocks[index]["content"], '\n-----------------')