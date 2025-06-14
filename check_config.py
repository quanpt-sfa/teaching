#!/usr/bin/env python3
"""
Auto-configuration checker for database schema grading system.

This script automatically detects and validates existing configuration.
"""

import os
import sys
from pathlib import Path

def check_configuration():
    """Check if system is already configured properly."""
    
    print("🔍 Checking system configuration...")
    
    config_found = False
    api_key_found = False
    
    # Check environment variables
    env_api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
    if env_api_key:
        print("✅ API key found in environment variables")
        api_key_found = True
        config_found = True
    
    # Check .env file
    env_file = Path(".env")
    if env_file.exists():
        print("✅ .env file found")
        config_found = True
        
        try:
            with open(env_file, 'r', encoding='utf-8') as f:
                content = f.read()
                if 'GOOGLE_API_KEY=' in content or 'GEMINI_API_KEY=' in content:
                    print("✅ API key found in .env file")
                    api_key_found = True
        except Exception:
            pass
    
    # Test if system works
    try:
        sys.path.insert(0, str(Path(__file__).parent))
        from v1.schema_grader.embedding.gemini import _API_AVAILABLE
        
        if _API_AVAILABLE:
            print("✅ Gemini API is working")
        else:
            print("ℹ️  Using fallback embedding (no API key)")
            
    except Exception as e:
        print(f"⚠️  Import test failed: {e}")
        return False
    
    if config_found:
        print("\n🎉 System is already configured and ready to use!")
        print("\n💡 Usage:")
        print("   python grade.py                    # Legacy interface") 
        print("   python v1/cli/cli.py batch folder/ # Modern CLI")
        return True
    else:
        print("\n📝 No configuration found. Running setup...")
        return False

def main():
    """Main function."""
    if check_configuration():
        # Already configured, offer to test
        if len(sys.argv) > 1 and sys.argv[1] == "--test":
            print("\n🧪 Running system test...")
            try:
                from v1.schema_grader.embedding.gemini import embed
                test_vec = embed("test")
                print(f"✅ Embedding test passed: {len(test_vec)} dimensions")
                print("🎉 System is working correctly!")
            except Exception as e:
                print(f"❌ Test failed: {e}")
        else:
            print("\n💡 To run a system test: python check_config.py --test")
    else:
        # Need setup
        print("Run: python setup.py")

if __name__ == "__main__":
    main()
