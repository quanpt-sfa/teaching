"""
Configuration management
"""

import os
import json
from typing import Dict, Any, Optional
from pathlib import Path


class Config:
    """Configuration manager for the grading system"""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize configuration"""
        self.config_path = config_path or os.getenv('GRADING_CONFIG', 'config/default.json')
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file"""
        config_file = Path(self.config_path)
        
        if config_file.exists():
            with open(config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        else:
            # Return default configuration
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration"""
        return {
            "database": {
                "answer_connection_string": "",
                "student_connection_string": "",
                "timeout": 30
            },
            "matching": {
                "similarity_threshold": 0.7,
                "fuzzy_threshold": 0.8,
                "max_suggestions": 5
            },
            "business_logic": {
                "expected_changes": {
                    "NhaCungCap": 1,
                    "NhanVien": 1,
                    "HangHoa": 1,
                    "MuaHang": 1,
                    "ChiTietMuaHang": 1
                }
            },
            "output": {
                "csv_encoding": "utf-8-sig",
                "excel_compatible": True
            },
            "logging": {
                "level": "INFO",
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            }
        }
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value"""
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
                
        return value
    
    def set(self, key: str, value: Any) -> None:
        """Set configuration value"""
        keys = key.split('.')
        config = self.config
        
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
            
        config[keys[-1]] = value
    
    def save(self, path: Optional[str] = None) -> None:
        """Save configuration to file"""
        save_path = path or self.config_path
        
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        
        with open(save_path, 'w', encoding='utf-8') as f:
            json.dump(self.config, f, indent=2, ensure_ascii=False)

# Alias for backward compatibility and cleaner imports
ConfigManager = Config

# Legacy function for backward compatibility
def get_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Get configuration dictionary - legacy function."""
    config_manager = Config(config_path)
    return config_manager.get_config()
