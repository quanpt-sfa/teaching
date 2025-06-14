#!/usr/bin/env python3
"""
Setup script for database schema grading system.

This script helps users configure API keys and environment variables.
"""

import os
import sys
from pathlib import Path

def create_env_file():
    """Create .env file interactively."""
    env_path = Path(".env")
    
    print("🚀 Database Schema Grading System Setup")
    print("=" * 50)
    
    if env_path.exists():
        overwrite = input("⚠️  .env file already exists. Overwrite? (y/N): ").lower()
        if overwrite != 'y':
            print("Setup cancelled.")
            return
    
    print("\n📝 Please provide the following information:")
    
    # Database configuration
    print("\n🗄️  Database Configuration:")
    db_server = input("Database server (default: localhost): ").strip() or "localhost"
    db_user = input("Database username (default: sa): ").strip() or "sa"
    db_password = input("Database password: ").strip()
    data_folder = input("Data folder (default: C:/temp/): ").strip() or "C:/temp/"
    
    # API configuration
    print("\n🤖 Google Gemini API Configuration:")
    print("   Get your API key from: https://aistudio.google.com/app/apikey")
    api_key = input("Google API Key (optional): ").strip()
    
    # Create .env content
    env_content = f"""# Database Configuration
DB_SERVER={db_server}
DB_USER={db_user}
DB_PASSWORD={db_password}
DATA_FOLDER={data_folder}
OUTPUT_FOLDER=results/

# Google Gemini API Configuration
GOOGLE_API_KEY={api_key}

# Optional Settings
USE_GEMINI_API={"true" if api_key else "false"}
ENABLE_DETAILED_LOGGING=false
"""
    
    # Write .env file
    with open(env_path, 'w', encoding='utf-8') as f:
        f.write(env_content)
    
    print(f"\n✅ Configuration saved to {env_path.absolute()}")
    
    if not api_key:
        print("\n⚠️  Warning: No Gemini API key provided.")
        print("   The system will use fallback embedding methods.")
        print("   For best results, get an API key from: https://aistudio.google.com/app/apikey")
    
    print("\n🎉 Setup complete! You can now run the grading system.")

def test_configuration():
    """Test the configuration."""
    try:
        # Test imports
        from v1.schema_grader import SchemaGrader
        from v1.schema_grader.config import GradingConfig
        
        print("✅ All imports successful")
        
        # Test configuration
        config = GradingConfig.from_env()
        print(f"✅ Configuration loaded: {config.server}")
        
        # Test API
        from v1.schema_grader.embedding.gemini import embed
        test_vec = embed("test")
        print(f"✅ Embedding test successful: {len(test_vec)} dimensions")
        
        print("\n🎉 All tests passed! System is ready to use.")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        print("\nPlease check your configuration and try again.")

def main():
    """Main setup function."""
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_configuration()
    else:
        create_env_file()

if __name__ == "__main__":
    main()
