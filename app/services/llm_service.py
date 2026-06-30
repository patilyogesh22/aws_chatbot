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


def synthesise_multi_file_answer(question: str, per_file_answers: list) -> str:
    """
    Combine answers from multiple files into one final response.
    """
    client = _get_client()

    parts = []

    for item in per_file_answers:
        file_label = item.get("file_name")
        if not file_label:
            file_label = ", ".join(item.get("file_names", []))

        file_type = item.get("file_type", "unknown")
        answer_text = item.get("answer", "No answer found.")

        parts.append(
            f"FILE(S): {file_label}\n"
            f"TYPE: {file_type}\n"
            f"ANSWER:\n{answer_text}"
        )

    combined_context = "\n\n---\n\n".join(parts)

    messages = [
        {
            "role": "system",
            "content": (
                "You are a data analyst. You are given answers from multiple uploaded files. "
                "Synthesize them into one clear final answer. Mention file names when useful. "
                "If files disagree or contain different information, explain the difference. "
                "Do not invent data that is not present in the provided per-file answers."
            ),
        },
        {
            "role": "user",
            "content": (
                f"User question:\n{question}\n\n"
                f"Per-file answers:\n{combined_context}\n\n"
                "Final synthesized answer:"
            ),
        },
    ]

    response = client.chat.completions.create(
        model=GROQ_MODEL,
        messages=messages,
        temperature=0.1,
        max_tokens=1024,
        stream=False,
    )

    return response.choices[0].message.content.strip()
