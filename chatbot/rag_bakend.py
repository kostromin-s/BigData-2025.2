import requests

OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "llama3.2:3b"


def llm_answer(question: str) -> str:
    try:
        res = requests.post(
            OLLAMA_URL,
            json={
                "model": MODEL,
                "prompt": question,
                "stream": False
            },
            timeout=60
        )

        return res.json()["response"]

    except Exception as e:
        return f"Lỗi gọi LLaMA local: {e}"