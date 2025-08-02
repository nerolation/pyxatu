"""Tests for configuration management."""

import os
import json
import tempfile
from pathlib import Path
from unittest.mock import patch, mock_open
import pytest
from pydantic import ValidationError, SecretStr

from pyxatu.config import (
    ClickhouseConfig, MempoolConfig, RelayConfig,
    PyXatuConfig, ConfigManager
)


class TestClickhouseConfig:
    """Test ClickHouse configuration."""
    
    def test_valid_config(self):
        """Test creating valid ClickHouse config."""
        config = ClickhouseConfig(
            url="https://clickhouse.example.com",
            user="testuser",
            password=SecretStr("testpass"),
            database="testdb",
            timeout=60,
            max_retries=5,
            pool_size=20
        )
        
        assert config.url == "https://clickhouse.example.com"
        assert config.user == "testuser"
        assert config.password.get_secret_value() == "testpass"
        assert config.database == "testdb"
        assert config.timeout == 60
        assert config.max_retries == 5
        assert config.pool_size == 20
        
    def test_url_validation(self):
        """Test URL validation and normalization."""
        # Valid URLs
        config = ClickhouseConfig(
            url="http://localhost:8123/",
            user="user",
            password="pass"
        )
        assert config.url == "http://localhost:8123"  # Trailing slash removed
        
        config = ClickhouseConfig(
            url="https://ch.example.com",
            user="user",
            password="pass"
        )
        assert config.url == "https://ch.example.com"
        
        # Invalid URLs
        with pytest.raises(ValidationError) as exc_info:
            ClickhouseConfig(
                url="clickhouse.example.com",  # Missing protocol
                user="user",
                password="pass"
            )
        assert "must start with http://" in str(exc_info.value)
        
    def test_password_security(self):
        """Test that passwords are handled securely."""
        config = ClickhouseConfig(
            url="https://ch.example.com",
            user="user",
            password="super-secret-password"
        )
        
        # Password should be SecretStr
        assert isinstance(config.password, SecretStr)
        
        # String representation should not expose password
        config_str = str(config)
        assert "super-secret-password" not in config_str
        
        # JSON representation should not expose password
        config_dict = config.model_dump()
        assert config_dict['password'] != "super-secret-password"
        
    def test_validation_ranges(self):
        """Test parameter validation ranges."""
        # Valid ranges
        ClickhouseConfig(
            url="https://ch.example.com",
            user="user",
            password="pass",
            timeout=1,  # Min
            max_retries=0,  # Min
            pool_size=1  # Min
        )
        
        ClickhouseConfig(
            url="https://ch.example.com",
            user="user",
            password="pass",
            timeout=3600,  # Max
            max_retries=10,  # Max
            pool_size=100  # Max
        )
        
        # Invalid ranges
        with pytest.raises(ValidationError):
            ClickhouseConfig(
                url="https://ch.example.com",
                user="user",
                password="pass",
                timeout=0  # Too small
            )
            
        with pytest.raises(ValidationError):
            ClickhouseConfig(
                url="https://ch.example.com",
                user="user",
                password="pass",
                pool_size=101  # Too large
            )
            
    def test_frozen_config(self):
        """Test that config is immutable."""
        config = ClickhouseConfig(
            url="https://ch.example.com",
            user="user",
            password="pass"
        )
        
        with pytest.raises(ValidationError):
            config.url = "https://new.example.com"


class TestMempoolConfig:
    """Test mempool configuration."""
    
    def test_default_config(self):
        """Test default mempool configuration."""
        config = MempoolConfig()
        
        assert config.flashbots_url == "https://mempool-dumpster.flashbots.net"
        assert config.blocknative_url == "https://api.blocknative.com/v0"
        assert config.cache_dir == Path.home() / ".pyxatu" / "mempool_cache"
        assert config.cache_ttl == 3600
        
    def test_custom_config(self):
        """Test custom mempool configuration."""
        custom_cache = Path("/tmp/custom_cache")
        config = MempoolConfig(
            flashbots_url="https://custom.flashbots.net",
            cache_dir=custom_cache,
            cache_ttl=7200
        )
        
        assert config.flashbots_url == "https://custom.flashbots.net"
        assert config.cache_dir == custom_cache
        assert config.cache_ttl == 7200
        
    def test_cache_ttl_validation(self):
        """Test cache TTL validation."""
        # Valid TTL
        MempoolConfig(cache_ttl=60)  # Min
        
        # Invalid TTL
        with pytest.raises(ValidationError):
            MempoolConfig(cache_ttl=30)  # Too small


class TestRelayConfig:
    """Test relay configuration."""
    
    def test_default_config(self):
        """Test default relay configuration."""
        config = RelayConfig()
        
        assert config.timeout == 30
        assert config.max_retries == 3
        assert "flashbots" in config.relay_endpoints
        assert "ultra_sound" in config.relay_endpoints
        assert len(config.relay_endpoints) >= 9  # Should have many relays
        
    def test_custom_relay_endpoints(self):
        """Test custom relay endpoints."""
        custom_endpoints = {
            "custom_relay": "https://custom.relay.com",
            "another_relay": "https://another.relay.com"
        }
        
        config = RelayConfig(
            timeout=60,
            relay_endpoints=custom_endpoints
        )
        
        assert config.timeout == 60
        assert config.relay_endpoints == custom_endpoints
        assert "flashbots" not in config.relay_endpoints  # Default replaced
        
    def test_validation_ranges(self):
        """Test relay config validation."""
        # Valid ranges
        RelayConfig(timeout=5, max_retries=0)
        RelayConfig(timeout=120, max_retries=5)
        
        # Invalid ranges
        with pytest.raises(ValidationError):
            RelayConfig(timeout=4)  # Too small
            
        with pytest.raises(ValidationError):
            RelayConfig(max_retries=6)  # Too large


class TestPyXatuConfig:
    """Test main PyXatu configuration."""
    
    def test_complete_config(self):
        """Test creating complete configuration."""
        config = PyXatuConfig(
            clickhouse=ClickhouseConfig(
                url="https://ch.example.com",
                user="user",
                password="pass"
            ),
            mempool=MempoolConfig(cache_ttl=1800),
            relay=RelayConfig(timeout=45),
            log_level="DEBUG",
            cache_enabled=False
        )
        
        assert config.clickhouse.url == "https://ch.example.com"
        assert config.mempool.cache_ttl == 1800
        assert config.relay.timeout == 45
        assert config.log_level == "DEBUG"
        assert not config.cache_enabled
        
    def test_log_level_validation(self):
        """Test log level validation."""
        # Valid levels
        for level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            PyXatuConfig(
                clickhouse=ClickhouseConfig(
                    url="https://ch.example.com",
                    user="user",
                    password="pass"
                ),
                log_level=level
            )
            
        # Invalid level
        with pytest.raises(ValidationError):
            PyXatuConfig(
                clickhouse=ClickhouseConfig(
                    url="https://ch.example.com",
                    user="user",
                    password="pass"
                ),
                log_level="INVALID"
            )
            
    def test_required_clickhouse(self):
        """Test that ClickHouse config is required."""
        with pytest.raises(ValidationError):
            PyXatuConfig()  # Missing required clickhouse config


class TestConfigManager:
    """Test configuration manager."""
    
    def test_load_from_file(self):
        """Test loading configuration from file."""
        config_data = {
            "clickhouse": {
                "url": "https://ch.example.com",
                "user": "fileuser",
                "password": "filepass"
            },
            "log_level": "WARNING"
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_data, f)
            temp_path = Path(f.name)
            
        try:
            manager = ConfigManager(config_path=temp_path, use_env_vars=False)
            config = manager.load()
            
            assert config.clickhouse.url == "https://ch.example.com"
            assert config.clickhouse.user == "fileuser"
            assert config.clickhouse.password.get_secret_value() == "filepass"
            assert config.log_level == "WARNING"
        finally:
            temp_path.unlink()
            
    def test_load_legacy_format(self):
        """Test loading legacy configuration format."""
        legacy_data = {
            "CLICKHOUSE_URL": "https://legacy.example.com",
            "CLICKHOUSE_USER": "legacyuser",
            "CLICKHOUSE_PASSWORD": "legacypass",
            "CLICKHOUSE_DATABASE": "legacydb"
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(legacy_data, f)
            temp_path = Path(f.name)
            
        try:
            manager = ConfigManager(config_path=temp_path, use_env_vars=False)
            config = manager.load()
            
            assert config.clickhouse.url == "https://legacy.example.com"
            assert config.clickhouse.user == "legacyuser"
            assert config.clickhouse.password.get_secret_value() == "legacypass"
            assert config.clickhouse.database == "legacydb"
        finally:
            temp_path.unlink()
            
    @patch.dict(os.environ, {
        'CLICKHOUSE_URL': 'https://env.example.com',
        'CLICKHOUSE_USER': 'envuser',
        'CLICKHOUSE_PASSWORD': 'envpass'
    })
    def test_load_from_env(self):
        """Test loading configuration from environment variables."""
        manager = ConfigManager(config_path=Path("/nonexistent"), use_env_vars=True)
        config = manager.load()
        
        assert config.clickhouse.url == "https://env.example.com"
        assert config.clickhouse.user == "envuser"
        assert config.clickhouse.password.get_secret_value() == "envpass"
        
    @patch.dict(os.environ, {
        'PYXATU_CLICKHOUSE_URL': 'https://pyxatu.example.com',
        'PYXATU_CLICKHOUSE_USER': 'pyxatuuser',
        'PYXATU_CLICKHOUSE_PASSWORD': 'pyxatupass',
        'PYXATU_CLICKHOUSE_DATABASE': 'pyxatudb',
        'PYXATU_LOG_LEVEL': 'ERROR'
    })
    def test_load_from_env_with_prefix(self):
        """Test loading configuration from environment with PYXATU_ prefix."""
        manager = ConfigManager(config_path=Path("/nonexistent"), use_env_vars=True)
        config = manager.load()
        
        assert config.clickhouse.url == "https://pyxatu.example.com"
        assert config.clickhouse.user == "pyxatuuser"
        assert config.clickhouse.password.get_secret_value() == "pyxatupass"
        assert config.clickhouse.database == "pyxatudb"
        assert config.log_level == "ERROR"
        
    @patch.dict(os.environ, {
        'CLICKHOUSE_URL': 'https://env.example.com',
        'CLICKHOUSE_PASSWORD': 'envpass'
    })
    def test_env_override_file(self):
        """Test that environment variables override file config."""
        file_data = {
            "clickhouse": {
                "url": "https://file.example.com",
                "user": "fileuser",
                "password": "filepass"
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(file_data, f)
            temp_path = Path(f.name)
            
        try:
            manager = ConfigManager(config_path=temp_path, use_env_vars=True)
            config = manager.load()
            
            # URL and password from env should override file
            assert config.clickhouse.url == "https://env.example.com"
            assert config.clickhouse.password.get_secret_value() == "envpass"
            # User gets default value since env creates complete clickhouse config
            assert config.clickhouse.user == "default"
        finally:
            temp_path.unlink()
            
    def test_missing_config(self):
        """Test error when no configuration is available."""
        manager = ConfigManager(config_path=Path("/nonexistent"), use_env_vars=False)
        
        with pytest.raises(ValueError, match="ClickHouse configuration is required"):
            manager.load()
            
    def test_get_config_value(self):
        """Test getting configuration values by key."""
        manager = ConfigManager(use_env_vars=False)
        manager._config = PyXatuConfig(
            clickhouse=ClickhouseConfig(
                url="https://ch.example.com",
                user="testuser",
                password="testpass"
            ),
            log_level="INFO"
        )
        
        # Test dot notation access
        assert manager.get('clickhouse.url') == "https://ch.example.com"
        assert manager.get('clickhouse.user') == "testuser"
        assert manager.get('clickhouse.password') == "testpass"  # SecretStr unwrapped
        assert manager.get('log_level') == "INFO"
        
        # Test default values
        assert manager.get('nonexistent', 'default') == 'default'
        assert manager.get('clickhouse.nonexistent', 'default') == 'default'
        
    def test_validate_config(self):
        """Test configuration validation."""
        # Valid config
        manager = ConfigManager(use_env_vars=False)
        manager._config = PyXatuConfig(
            clickhouse=ClickhouseConfig(
                url="https://ch.example.com",
                user="user",
                password="pass"
            )
        )
        assert manager.validate() is True
        
        # Invalid config (will fail to load)
        manager2 = ConfigManager(config_path=Path("/nonexistent"), use_env_vars=False)
        assert manager2.validate() is False
        
    def test_save_template(self):
        """Test saving configuration template."""
        with tempfile.TemporaryDirectory() as tmpdir:
            template_path = Path(tmpdir) / "template.json"
            
            manager = ConfigManager()
            manager.save_template(template_path)
            
            assert template_path.exists()
            
            with open(template_path) as f:
                template = json.load(f)
                
            assert "clickhouse" in template
            assert template["clickhouse"]["url"] == "https://your-clickhouse-server.com"
            assert template["clickhouse"]["user"] == "your_username"
            assert template["clickhouse"]["password"] == "your_password"
            
    def test_immutable_config(self):
        """Test that configuration cannot be modified after loading."""
        manager = ConfigManager(use_env_vars=False)
        manager._config = PyXatuConfig(
            clickhouse=ClickhouseConfig(
                url="https://ch.example.com",
                user="user",
                password="pass"
            )
        )
        
        with pytest.raises(NotImplementedError):
            manager.set('clickhouse.url', 'https://new.example.com')