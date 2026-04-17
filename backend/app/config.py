"""Application configuration using Pydantic Settings."""

import os
import logging
from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Environment
    environment: str = "development"

    # Databricks Configuration
    databricks_host: str = ""  # Auto-provided in Databricks Apps
    databricks_http_path: str = ""  # For local dev with full path
    databricks_warehouse_id: str = ""  # From SQL warehouse resource in Databricks Apps
    databricks_token: Optional[str] = None  # Used for local dev only
    
    # OAuth credentials (auto-provided in Databricks Apps)
    databricks_client_id: Optional[str] = None
    databricks_client_secret: Optional[str] = None

    # App Configuration
    app_name: str = "CSM Dashboard"
    debug: bool = False

    # CSM feedback drill-out (optional). E.g. https://yourorg.freshdesk.com — ticket link: {base}/a/tickets/{id}
    freshdesk_portal_base: str = ""
    # E.g. https://example.com/path/{response_id} for SurveyMonkey response deep links
    survey_monkey_response_url_template: str = ""

    # SendGrid Email Configuration
    # Key expires 2026-07-03 — raise renewal incident 2 weeks prior
    sendgrid_api_key: str = ""
    sendgrid_from_email: str = "do-not-reply-iona@ifs.com"
    sendgrid_from_name: str = "Iona CSM"

    @property
    def is_development(self) -> bool:
        return self.environment == "development"

    @property
    def is_production(self) -> bool:
        return self.environment == "production"

    @property
    def effective_http_path(self) -> str:
        """Get the effective HTTP path - either direct or constructed from warehouse ID."""
        if self.databricks_http_path:
            return self.databricks_http_path
        if self.databricks_warehouse_id:
            return f"/sql/1.0/warehouses/{self.databricks_warehouse_id}"
        return ""
    
    def log_config_status(self):
        """Log configuration status for debugging."""
        logger.info(f"=== Databricks Configuration Status ===")
        logger.info(f"Environment: {self.environment}")
        logger.info(f"DATABRICKS_HOST: {'SET' if self.databricks_host else 'NOT SET'} ({self.databricks_host[:30] + '...' if self.databricks_host and len(self.databricks_host) > 30 else self.databricks_host})")
        logger.info(f"DATABRICKS_WAREHOUSE_ID: {'SET' if self.databricks_warehouse_id else 'NOT SET'} ({self.databricks_warehouse_id})")
        logger.info(f"DATABRICKS_HTTP_PATH: {'SET' if self.databricks_http_path else 'NOT SET'}")
        logger.info(f"Effective HTTP Path: {self.effective_http_path}")
        logger.info(f"DATABRICKS_TOKEN: {'SET' if self.databricks_token else 'NOT SET'}")
        logger.info(f"DATABRICKS_CLIENT_ID: {'SET' if self.databricks_client_id else 'NOT SET'}")
        logger.info(f"DATABRICKS_CLIENT_SECRET: {'SET' if self.databricks_client_secret else 'NOT SET'}")
        logger.info(f"========================================")


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    settings = Settings()
    # Log config on first load
    settings.log_config_status()
    return settings
