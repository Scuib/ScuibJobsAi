"""
main.py — entry point.
Run: uvicorn main:app --reload
"""

from dotenv import load_dotenv
import os
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

from api.app import app  # noqa: E402 — must load env before importing app
