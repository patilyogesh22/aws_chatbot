"""
app/services/ai_fallback_service.py

Common AI wrapper:
1. Try Groq primary model.
2. Try Groq fallback model.
3. If Groq quota/rate limit is reached, fallback to Gemini.
4. If all fail, raise a clean error message.

This file does not change your chat, RAG, structured SQL, or multi-file logic.
"""

import os
from groq import Groq
from google import genai

from app.config import GROQ_API_KEY, GROQ_MODEL


_groq_client = None
_gemini_client = None


def _get_groq_client():
    global _groq_client

    if _groq_client is None:
        if not GROQ_API_KEY:
            raise ValueError("GROQ_API_KEY is not set")
        _groq_client = Groq(api_key=GROQ_API_KEY)

    return _groq_client


def _get_gemini_client():
    global _gemini_client

    gemini_api_key = os.getenv("GEMINI_API_KEY")

    if not gemini_api_key:
        return None

    if _gemini_client is None:
        _gemini_client = genai.Client(api_key=gemini_api_key)

    return _gemini_client


def is_rate_limit_error(error: Exception) -> bool:
    msg = str(error)

    return (
        "rate_limit_exceeded" in msg
        or "Rate limit reached" in msg
        or "429" in msg
        or "tokens per day" in msg
        or "TPD" in msg
        or "quota" in msg.lower()
    )


def _messages_to_gemini_prompt(messages: list[dict]) -> str:
    parts = []

    for msg in messages:
        role = msg.get("role", "user")
        content = msg.get("content", "")
        parts.append(f"{role.upper()}:\n{content}")

    return "\n\n".join(parts)


def call_ai_with_fallback(
    *,
    messages: list[dict],
    temperature: float = 0.2,
    max_tokens: int = 600,
) -> str:
    groq_models = []

    if GROQ_MODEL:
        groq_models.append(GROQ_MODEL)

    groq_fallback_model = os.getenv("GROQ_FALLBACK_MODEL")
    if groq_fallback_model and groq_fallback_model not in groq_models:
        groq_models.append(groq_fallback_model)

    groq_client = _get_groq_client()
    last_error = None

    for model in groq_models:
        try:
            response = groq_client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
                stream=False,
            )
            return response.choices[0].message.content.strip()

        except Exception as e:
            last_error = e

            if is_rate_limit_error(e):
                print(f"[AI fallback] Groq model limited: {model}")
                continue

            raise e

    gemini_client = _get_gemini_client()

    if gemini_client is None:
        raise Exception(
            "AI quota limit reached. Gemini fallback is not configured. "
            "Please add GEMINI_API_KEY or try again later."
        )

    try:
        gemini_model = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")
        prompt = _messages_to_gemini_prompt(messages)

        response = gemini_client.models.generate_content(
            model=gemini_model,
            contents=prompt,
        )

        text = getattr(response, "text", None)
        if not text:
            raise Exception("Gemini returned an empty response")

        print("[AI fallback] Gemini fallback used successfully")
        return text.strip()

    except Exception as gemini_error:
        raise Exception(
            "AI quota limit reached and Gemini fallback also failed. "
            f"Gemini error: {str(gemini_error)}"
        ) from last_error
