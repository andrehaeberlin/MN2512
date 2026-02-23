import os
from env_check import openai_key_exists, openai_key_prefix

def test_openai_key_exists():
    assert openai_key_exists() is True, "OPENAI_API_KEY não está configurada"

def test_openai_key_prefix():
    prefix = openai_key_prefix()
    assert prefix is not None
    assert prefix.startswith("sk-")