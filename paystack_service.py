from __future__ import annotations

import hashlib
import hmac
import json
import os
from typing import Any

import dotenv
import requests


class PaystackService:
    BASE_URL = "https://api.paystack.co"

    def __init__(self, secret_key: str | None = None, timeout: int = 20) -> None:
        env = dotenv.dotenv_values(".env")
        self.secret_key = secret_key or env.get("PAYSTACK_SECRET_KEY") or os.getenv("PAYSTACK_SECRET_KEY")
        if not self.secret_key:
            raise ValueError("Missing PAYSTACK_SECRET_KEY")
        self.timeout = timeout

    @property
    def headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.secret_key}",
            "Content-Type": "application/json",
        }

    def initialize_transaction(
        self,
        *,
        email: str,
        amount: int,
        reference: str,
        callback_url: str | None = None,
        plan_code: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "email": email,
            "amount": amount,
            "reference": reference,
        }
        if callback_url:
            payload["callback_url"] = callback_url
        if plan_code:
            payload["plan"] = plan_code
        if metadata:
            payload["metadata"] = metadata

        response = requests.post(
            f"{self.BASE_URL}/transaction/initialize",
            headers=self.headers,
            json=payload,
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        if not data.get("status"):
            raise RuntimeError(data.get("message", "Paystack initialization failed"))
        return data

    def verify_transaction(self, reference: str) -> dict[str, Any]:
        response = requests.get(
            f"{self.BASE_URL}/transaction/verify/{reference}",
            headers=self.headers,
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        if not data.get("status"):
            raise RuntimeError(data.get("message", "Paystack verification failed"))
        return data

    def validate_webhook_signature(self, raw_body: bytes, signature: str | None) -> bool:
        if not signature:
            return False
        computed = hmac.new(
            self.secret_key.encode("utf-8"),
            msg=raw_body,
            digestmod=hashlib.sha512,
        ).hexdigest()
        return hmac.compare_digest(computed, signature)

    @staticmethod
    def parse_event(raw_body: bytes) -> dict[str, Any]:
        return json.loads(raw_body.decode("utf-8"))
