import json
import re
import logging
import os
from typing import Dict, Any, Optional
from langchain_google_genai import ChatGoogleGenerativeAI
from src.utils.config import GEMINI_API_KEY

logger = logging.getLogger(__name__)

def get_llm(model_name: str = "gemini-2.0-flash", api_key: Optional[str] = None):
    api_key = api_key or GEMINI_API_KEY
    if not api_key:
        raise ValueError("GEMINI_API_KEY is required in config.py or environment variables")
    return ChatGoogleGenerativeAI(model=model_name, google_api_key=api_key, temperature=0)

def parse_json_safely(content: Any) -> Dict[str, Any]:
    """Robustly extracts and parses JSON from LLM response."""
    if isinstance(content, dict) and "text" in content:
        content = content["text"]
    elif isinstance(content, list):
        content = "".join([str(p.get("text", p)) if isinstance(p, dict) else str(p) for p in content])
    
    if not isinstance(content, str):
        content = str(content)
    
    # 1. Try to find JSON within code blocks first
    code_block_match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", content, re.DOTALL)
    if code_block_match:
        content = code_block_match.group(1)
    else:
        # 2. Extract anything between the first { and last }
        start = content.find('{')
        end = content.rfind('}')
        if start != -1 and end != -1:
            content = content[start:end+1]
    
    # 3. Last-ditch cleanup
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        # Try fixing common single quote issues
        fixed_content = re.sub(r"\'(\w+)\'\s*:", r"\"\1\":", content)
        fixed_content = re.sub(r":\s*\'(.*?)\'", r": \"\1\"", fixed_content)
        try:
            return json.loads(fixed_content)
        except:
            logger.error(f"JSON Parsing Error. Content: {content[:100]}...")
            return {}
