import requests
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.config import settings

OLLAMA_URL = settings.OLLAMA_URL
MODEL = settings.MODEL

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