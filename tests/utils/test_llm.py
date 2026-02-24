"""Tests for LLM-powered transformation utilities."""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from spark_fuse.utils.llm import _get_llm_api_config


class TestGetLlmApiConfig:
    def test_openai_standard(self):
        env = {"OPENAI_API_KEY": "sk-test123"}
        with patch.dict(os.environ, env, clear=True):
            url, headers, use_azure = _get_llm_api_config("gpt-4")

        assert url == "https://api.openai.com/v1/chat/completions"
        assert "Authorization" in headers
        assert headers["Authorization"] == "Bearer sk-test123"
        assert use_azure is False

    def test_azure_openai(self):
        env = {
            "AZURE_OPENAI_KEY": "azure-key-123",
            "AZURE_OPENAI_ENDPOINT": "https://myresource.openai.azure.com",
            "AZURE_OPENAI_API_VERSION": "2024-02-01",
        }
        with patch.dict(os.environ, env, clear=True):
            url, headers, use_azure = _get_llm_api_config("my-deployment")

        assert "my-deployment" in url
        assert "api-version=2024-02-01" in url
        assert headers["api-key"] == "azure-key-123"
        assert use_azure is True

    def test_azure_fallback_version(self):
        env = {
            "AZURE_OPENAI_API_KEY": "key",
            "AZURE_OPENAI_ENDPOINT": "https://myresource.openai.azure.com/",
        }
        with patch.dict(os.environ, env, clear=True):
            url, headers, use_azure = _get_llm_api_config("deploy")

        assert "api-version=2023-05-15" in url
        assert use_azure is True

    def test_missing_key_raises(self):
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(RuntimeError, match="API key not found"):
                _get_llm_api_config("gpt-4")

    def test_openai_api_base_treated_as_azure(self):
        env = {
            "OPENAI_API_KEY": "key",
            "OPENAI_API_BASE": "https://alt-endpoint.openai.azure.com",
        }
        with patch.dict(os.environ, env, clear=True):
            url, headers, use_azure = _get_llm_api_config("deploy")

        assert use_azure is True
        assert "alt-endpoint" in url
