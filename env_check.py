import os

from dotenv import load_dotenv


load_dotenv()

def openai_key_exists():
    return bool(os.getenv("OPENAI_API_KEY"))

def openai_key_prefix():
    key = os.getenv("OPENAI_API_KEY")
    return key[:7] if key else None