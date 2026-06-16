"""
llm.py
Groq LLM integration for answering questions using retrieved context.
"""
from groq import Groq
from app.config import GROQ_API_KEY, GROQ_MODEL

_client: Groq = None


def _get_client() -> Groq:
    global _client
    if _client is None:
        if not GROQ_API_KEY:
            raise ValueError(
                "GROQ_API_KEY is not set. "
                "Get a free key at https://console.groq.com"
            )
        _client = Groq(api_key=GROQ_API_KEY)
    return _client


SYSTEM_PROMPT = """You are a helpful AI assistant that answers questions \
based ONLY on the provided document context.

Rules:
1. Answer only from the context provided — do not use outside knowledge.
2. If the answer is not in the context, say: "I could not find the answer in \
the provided documents."
3. Be concise, accurate, and cite which file/chunk your answer comes from \
when relevant.
4. If the question is ambiguous, ask for clarification.
"""


def answer(question: str, context: str,
           chat_history: list = None) -> str:
    """
    Send question + context to Groq and return the answer string.

    Args:
        question:     User's question.
        context:      Retrieved document chunks as a string.
        chat_history: Optional list of prior {"role","content"} messages.

    Returns:
        Assistant's answer as a string.
    """
    client = _get_client()

    messages = [{"role": "system", "content": SYSTEM_PROMPT}]

    # Include prior conversation turns if provided
    if chat_history:
        messages.extend(chat_history)

    user_message = (
        f"DOCUMENT CONTEXT:\n{context}\n\n"
        f"QUESTION: {question}"
    )
    messages.append({"role": "user", "content": user_message})

    response = client.chat.completions.create(
        model=GROQ_MODEL,
        messages=messages,
        temperature=0.2,
        max_tokens=1024,
        stream=False,
    )

    return response.choices[0].message.content.strip()


def answer_stream(question: str, context: str):
    """
    Streaming version — yields text chunks as they arrive from Groq.
    Use this in the Streamlit UI for a better UX.
    """
    client = _get_client()

    messages = [
        {"role": "system",  "content": SYSTEM_PROMPT},
        {"role": "user",    "content": (
            f"DOCUMENT CONTEXT:\n{context}\n\n"
            f"QUESTION: {question}"
        )},
    ]

    stream = client.chat.completions.create(
        model=GROQ_MODEL,
        messages=messages,
        temperature=0.2,
        max_tokens=1024,
        stream=True,
    )

    for chunk in stream:
        delta = chunk.choices[0].delta.content
        if delta:
            yield delta