#!/usr/bin/env python3
"""
Quick system status check for the grading system.
"""

import os
import sys
sys.path.append('v1')

from v1.schema_grader.utils.constants import API_KEY
from v1.schema_grader.embedding.gemini import _initialize_gemini, is_api_available
from v1.schema_grader.utils.log import get_logger

def check_system_status():
    """Check the current system configuration and API status."""
    logger = get_logger(__name__)
    
    print("=== Database Schema Grading System Status ===")
    print()
    
    # Check API Key Configuration
    print("1. API Key Configuration:")
    env_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
    env_file = ".env"
    
    if env_key:
        print(f"   ✓ Environment variable found: {env_key[:10]}...")
    elif os.path.exists(env_file):
        print(f"   ✓ .env file found")
    else:
        print(f"   ! Using legacy fallback key: {API_KEY[:10]}...")
    
    print(f"   Current API Key: {API_KEY[:10]}...")
    print()
    
    # Check API Availability
    print("2. Gemini API Status:")
    try:
        _initialize_gemini()
        if is_api_available():
            print("   ✓ Gemini API is available and working")
        else:
            print("   ! Gemini API not available - using hash-based fallback")
    except Exception as e:
        print(f"   ✗ Error checking API: {e}")
        print("   ! Will use hash-based fallback")
    
    print()
    
    # Check Dependencies
    print("3. Dependencies:")
    try:
        import google.generativeai
        print("   ✓ google-generativeai installed")
    except ImportError:
        print("   ! google-generativeai not installed")
    
    try:
        import numpy
        print("   ✓ numpy installed")
    except ImportError:
        print("   ✗ numpy not installed")
    
    try:
        import fuzzywuzzy
        print("   ✓ fuzzywuzzy installed")
    except ImportError:
        print("   ✗ fuzzywuzzy not installed")
    
    print()
    
    # Summary
    print("4. Summary:")
    if is_api_available():
        print("   ✓ System fully operational with Gemini API")
        print("   → No setup required unless you want to change API key")
    else:
        print("   ⚠ System operational with fallback mode")
        print("   → Will work but semantic matching may be less accurate")
        print("   → Consider setting up Gemini API key for better results")
    
    print()
    print("=== End Status Report ===")

if __name__ == "__main__":
    check_system_status()
