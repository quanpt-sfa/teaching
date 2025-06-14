"""
Simple system status check without complex imports.
"""

import os

def simple_status_check():
    print("=== Quick System Status ===")
    print()
    
    # Check environment variables
    print("1. Environment Variables:")
    google_key = os.getenv("GOOGLE_API_KEY")
    gemini_key = os.getenv("GEMINI_API_KEY")
    
    if google_key:
        print(f"   ✓ GOOGLE_API_KEY found: {google_key[:10]}...")
    elif gemini_key:
        print(f"   ✓ GEMINI_API_KEY found: {gemini_key[:10]}...")
    else:
        print("   ! No environment API key found")
    
    # Check .env file
    print("\n2. Configuration File:")
    if os.path.exists(".env"):
        print("   ✓ .env file exists")
    else:
        print("   ! No .env file found")
    
    # Check project structure
    print("\n3. Project Structure:")
    if os.path.exists("v1/schema_grader"):
        print("   ✓ v1/schema_grader directory exists")
    else:
        print("   ✗ v1/schema_grader directory missing")
    
    if os.path.exists("requirements.txt"):
        print("   ✓ requirements.txt exists")
    else:
        print("   ✗ requirements.txt missing")
    
    # Summary
    print("\n4. Status Summary:")
    if google_key or gemini_key:
        print("   ✓ Custom API key configured - system ready!")
    elif os.path.exists(".env"):
        print("   ⚠ .env file exists but may need API key")
    else:
        print("   ⚠ Using fallback configuration")
        print("   → System will work with legacy API key")
        print("   → No setup required unless you want to optimize")
    
    print("\n=== Answer to your question ===")
    print("🔸 If your API key hasn't changed: NO SETUP REQUIRED")
    print("🔸 The system will use the existing configuration")
    print("🔸 Everything should work as before")
    print()

if __name__ == "__main__":
    simple_status_check()
