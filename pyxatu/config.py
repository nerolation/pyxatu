"""Configuration management for PyXatu."""

import os
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field, SecretStr, field_validator, ConfigDict
from pyxatu.base import ConfigProvider


class ClickhouseConfig(BaseModel):
    """ClickHouse connection configuration."""
    model_config = ConfigDict(frozen=True)
    
    url: str = Field(...)
    user: str = Field("default")
    password: SecretStr = Field(...)
    database: str = Field("default")
    timeout: int = Field(1500, ge=1, le=3600)
    max_retries: int = Field(3, ge=0, le=10)
    pool_size: int = Field(10, ge=1, le=100)
    
    @field_validator('url')
    @classmethod
    def validate_url(cls, v: str) -> str:
        """Validate URL format."""
        if not v.startswith(('http://', 'https://')):
            raise ValueError("URL must start with http:// or https://")
        return v.rstrip('/')


class MempoolConfig(BaseModel):
    """Mempool connector configuration."""
    model_config = ConfigDict(frozen=True)
    
    flashbots_url: str = Field("https://mempool-dumpster.flashbots.net")
    blocknative_url: str = Field("https://api.blocknative.com/v0")
    cache_dir: Path = Field(Path.home() / ".pyxatu" / "mempool_cache")
    cache_ttl: int = Field(3600, ge=60)


class RelayConfig(BaseModel):
    """MEV relay configuration."""
    model_config = ConfigDict(frozen=True)
    
    timeout: int = Field(30, ge=5, le=120, description="Relay request timeout")
    max_retries: int = Field(3, ge=0, le=5, description="Maximum retry attempts")
    relay_endpoints: Dict[str, str] = Field(
        default_factory=lambda: {
            "flashbots": "https://boost-relay.flashbots.net",
            "bloxroute_regulated": "https://bloxroute.regulated.blxrbdn.com",
            "bloxroute_max_profit": "https://bloxroute.max-profit.blxrbdn.com",
            "blocknative": "https://builder-relay-mainnet.blocknative.com",
            "eden": "https://relay.edennetwork.io",
            "manifold": "https://mainnet-relay.securerpc.com",
            "ultra_sound": "https://relay.ultrasound.money",
            "agnostic": "https://agnostic-relay.net",
            "aestus": "https://mainnet.aestus.live",
        },
        description="MEV relay endpoints"
    )


class PyXatuConfig(BaseModel):
    """Main PyXatu configuration."""
    model_config = ConfigDict(frozen=True)
    
    clickhouse: ClickhouseConfig
    mempool: MempoolConfig = Field(default_factory=MempoolConfig)
    relay: RelayConfig = Field(default_factory=RelayConfig)
    log_level: str = Field("INFO", pattern="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$")
    cache_enabled: bool = Field(True, description="Enable caching")
    cache_dir: Path = Field(
        Path.home() / ".pyxatu" / "cache",
        description="Main cache directory"
    )


class ConfigManager(ConfigProvider):
    """Configuration manager with validation and environment variable support."""
    
    def __init__(
        self,
        config_path: Optional[Path] = None,
        use_env_vars: bool = True
    ):
        self.config_path = config_path or Path.home() / ".pyxatu_config.json"
        self.use_env_vars = use_env_vars
        self._config: Optional[PyXatuConfig] = None
        self.logger = logging.getLogger(__name__)
        
    def load(self) -> PyXatuConfig:
        """Load configuration from file and/or environment variables."""
        config_dict = {}
        
        # Load from file if exists
        if self.config_path.exists():
            try:
                with open(self.config_path, 'r') as f:
                    file_config = json.load(f)
                config_dict = self._normalize_config(file_config)
            except Exception as e:
                self.logger.warning(f"Failed to load config from {self.config_path}: {e}")
        
        # Override with environment variables if enabled
        if self.use_env_vars:
            env_config = self._load_from_env()
            config_dict = self._merge_configs(config_dict, env_config)
        
        # Validate and create config object
        if not config_dict.get('clickhouse'):
            raise ValueError("ClickHouse configuration is required")
            
        self._config = PyXatuConfig(**config_dict)
        return self._config
    
    def _normalize_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize legacy config format to new structure."""
        # Handle old flat structure
        if 'CLICKHOUSE_URL' in config:
            return {
                'clickhouse': {
                    'url': config.get('CLICKHOUSE_URL'),
                    'user': config.get('CLICKHOUSE_USER', 'default'),
                    'password': config.get('CLICKHOUSE_PASSWORD'),
                    'database': config.get('CLICKHOUSE_DATABASE', 'default'),
                }
            }
        return config
    
    def _load_from_env(self) -> Dict[str, Any]:
        """Load configuration from environment variables."""
        config = {}
        
        # ClickHouse config
        ch_url = os.getenv('CLICKHOUSE_URL') or os.getenv('PYXATU_CLICKHOUSE_URL')
        ch_user = os.getenv('CLICKHOUSE_USER') or os.getenv('PYXATU_CLICKHOUSE_USER')
        ch_pass = os.getenv('CLICKHOUSE_PASSWORD') or os.getenv('PYXATU_CLICKHOUSE_PASSWORD')
        
        if ch_url and ch_pass:
            config['clickhouse'] = {
                'url': ch_url,
                'user': ch_user or 'default',
                'password': ch_pass,
            }
            
        # Optional configs
        if db := os.getenv('PYXATU_CLICKHOUSE_DATABASE'):
            config.setdefault('clickhouse', {})['database'] = db
            
        if log_level := os.getenv('PYXATU_LOG_LEVEL'):
            config['log_level'] = log_level
            
        return config
    
    def _merge_configs(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively merge configuration dictionaries."""
        result = base.copy()
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_configs(result[key], value)
            else:
                result[key] = value
                
        return result
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by dot-notation key."""
        if not self._config:
            self.load()
            
        parts = key.split('.')
        value = self._config
        
        for part in parts:
            if hasattr(value, part):
                value = getattr(value, part)
            else:
                return default
                
        # Handle SecretStr
        if hasattr(value, 'get_secret_value'):
            return value.get_secret_value()
            
        return value
    
    def set(self, key: str, value: Any) -> None:
        """Set configuration value (not supported for immutable config)."""
        raise NotImplementedError("Configuration is immutable after loading")
    
    def validate(self) -> bool:
        """Validate current configuration."""
        try:
            if not self._config:
                self.load()
            return True
        except Exception as e:
            self.logger.error(f"Configuration validation failed: {e}")
            return False
    
    def save_template(self, path: Optional[Path] = None) -> None:
        """Save a configuration template file."""
        template = {
            "clickhouse": {
                "url": "https://your-clickhouse-server.com",
                "user": "your_username",
                "password": "your_password",
                "database": "default"
            },
            "log_level": "INFO",
            "cache_enabled": True
        }
        
        save_path = path or self.config_path
        save_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(save_path, 'w') as f:
            json.dump(template, f, indent=2)
            
        self.logger.info(f"Configuration template saved to {save_path}")