from pipeline.chatbot import Chatbot

if __name__ == "__main__":
    chatbot = Chatbot()

    while (True):
        user_query = input("> Câu hỏi: ")
        if (user_query in ['quit', 'exit']):
            break

        answer, evidence = chatbot.answer(user_query)
        if (answer is None): 
            answer = "Không đủ dữ kiện!"
            
        print("> Trả lời:", answer)
        if evidence:
            print(" # Nguồn tham khảo:")
            for i, content in enumerate(evidence):
                print(f"Nguồn {i + 1}.\n", content, '\n-----------------')