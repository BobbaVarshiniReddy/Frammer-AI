import google.generativeai as genai
import os
from dotenv import load_dotenv

load_dotenv()

def get_gemini_model(model_name="gemini-1.5-flash"):
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        raise ValueError("GOOGLE_API_KEY not found in environment variables")
    
    genai.configure(api_key=api_key)
    return genai.GenerativeModel(model_name)
