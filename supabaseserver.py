
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import dotenv
from supabase import Client, create_client

#ENV = dotenv.dotenv_values(".env")
SUPABASE_URL = "https://egsgrqgzdmevupuchwbr.supabase.co"# ENV.get("SUPABASE_URL") or ""
# SUPABASE_PUBLISHABLE_KEY = (
#     ENV.get("SUPABASE_PUBLISHABLE_KEY")
#     or ENV.get("SUPABASE_ANON_KEY")
#     or ENV.get("SUPABASE_KEY")
#     or ""
# )
SUPABASE_SECRET_KEY ="sb_secret_Ya1yzrG3TkXiWOXp6hPDrQ_yYEIWHI4" #(
#     ENV.get("SUPABASE_SECRET_KEY")
#     or ENV.get("SUPABASE_SERVICE_ROLE_KEY")
#     or ENV.get("SUPABASE_KEY")
#     or ""
# )

PLAN_LIMITS = {
    "starter": 10,
    "growth": 25,
    "pro": 50,
}
VALID_PLANS = set(PLAN_LIMITS.keys())
VALID_PILOT_STATUSES = {"active", "paused", "expired"}
VALID_BILLING_STATUSES = {"inactive", "active", "past_due", "canceled"}


def has_paid_access(brand: dict[str, Any] | None) -> bool:
    if not brand:
        return False
    return str(brand.get("billing_status", "inactive")) == "active"


class StorageService:
    def __init__(
        self,
        supabase_url: str = SUPABASE_URL,
        publishable_key: str = SUPABASE_PUBLISHABLE_KEY,
        secret_key: str = SUPABASE_SECRET_KEY,
    ) -> None:
        if not supabase_url:
            raise ValueError("Missing SUPABASE_URL")
        if not publishable_key:
            raise ValueError("Missing SUPABASE_PUBLISHABLE_KEY or SUPABASE_ANON_KEY")
        if not secret_key:
            raise ValueError("Missing SUPABASE_SECRET_KEY or SUPABASE_SERVICE_ROLE_KEY")

        self.public: Client = create_client(supabase_url, publishable_key)
        self.admin: Client = create_client(supabase_url, secret_key)

    # ================================================================
    # Helpers
    # ================================================================

    @staticmethod
    def _to_dict(value: Any) -> dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, dict):
            return value
        if hasattr(value, "model_dump"):
            return value.model_dump()
        if hasattr(value, "__dict__"):
            return {k: v for k, v in value.__dict__.items() if not k.startswith("_")}
        return {}

    @staticmethod
    def _first_or_none(response: Any) -> dict[str, Any] | None:
        data = getattr(response, "data", None)
        if isinstance(data, list):
            return data[0] if data else None
        if isinstance(data, dict):
            return data
        return None

    @staticmethod
    def _parse_datetime(value: Any) -> datetime | None:
        if not value:
            return None
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc)
        if isinstance(value, str):
            try:
                cleaned = value.replace("Z", "+00:00")
                parsed = datetime.fromisoformat(cleaned)
                if parsed.tzinfo is None:
                    return parsed.replace(tzinfo=timezone.utc)
                return parsed.astimezone(timezone.utc)
            except ValueError:
                return None
        return None

    @staticmethod
    def _normalize_plan(plan: str | None) -> str:
        plan_value = (plan or "starter").strip().lower()
        if plan_value not in VALID_PLANS:
            raise ValueError(f"Invalid plan: {plan}")
        return plan_value

    @staticmethod
    def _clean_optional_str(value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        return cleaned or None

    # ================================================================
    # ACCESS / PILOT RULES
    # ================================================================

    def is_pilot_active(self, brand: dict[str, Any] | None) -> bool:
        if not brand:
            return False
        if not bool(brand.get("is_pilot", False)):
            return False
        if brand.get("pilot_status") != "active":
            return False

        expires_at = self._parse_datetime(brand.get("pilot_expires_at"))

        # No expiry means active pilot access
        if expires_at is None:
            return True

        return expires_at > datetime.now(timezone.utc)

    def has_active_access(self, brand: dict[str, Any] | None) -> bool:
        return has_paid_access(brand) or self.is_pilot_active(brand)

    def get_brand_search_limit_from_profile(self, brand: dict[str, Any]) -> int:
        if self.is_pilot_active(brand) and brand.get("max_searches_override") is not None:
            try:
                override = int(brand["max_searches_override"])
                if override > 0:
                    return override
            except (TypeError, ValueError):
                pass

        plan = str(brand.get("plan", "starter"))
        return PLAN_LIMITS.get(plan, PLAN_LIMITS["starter"])

    def get_access_summary(self, brand: dict[str, Any] | None) -> dict[str, Any]:
        if not brand:
            return {
                "has_access": False,
                "billing_status": "inactive",
                "is_pilot": False,
                "pilot_status": None,
                "pilot_active": False,
                "pilot_expires_at": None,
                "reason": "Brand not found",
            }

        paid = has_paid_access(brand)
        pilot_active = self.is_pilot_active(brand)
        has_access = paid or pilot_active
        if paid:
            reason = "Paid access active"
        elif pilot_active:
            reason = "Pilot access active"
        else:
            reason = "No active access — complete payment or receive pilot access"

        return {
            "has_access": has_access,
            "billing_status": brand.get("billing_status", "inactive"),
            "is_pilot": bool(brand.get("is_pilot", False)),
            "pilot_status": brand.get("pilot_status"),
            "pilot_active": pilot_active,
            "pilot_expires_at": brand.get("pilot_expires_at"),
            "reason": reason,
        }

    # ================================================================
    # AUTH
    # ================================================================

    def register_brand(self, email: str, password: str, full_name: str, company_name: str) -> dict[str, Any]:
        try:
            auth_response = self.public.auth.sign_up(
                {
                    "email": email,
                    "password": password,
                    "options": {"data": {"full_name": full_name}},
                }
            )
            user = self._to_dict(getattr(auth_response, "user", None))
            user_id = user.get("id")
            if not user_id:
                raise RuntimeError("Signup failed: no user returned")

            self.admin.table("brands").upsert(
                {
                    "id": user_id,
                    "email": email,
                    "full_name": full_name,
                    "company_name": company_name,
                    "plan": "starter",
                    "searches_used": 0,
                    "billing_status": "inactive",
                    "is_pilot": False,
                    "pilot_status": None,
                    "pilot_expires_at": None,
                    "max_searches_override": None,
                },
                on_conflict="id",
            ).execute()

            session = self._to_dict(getattr(auth_response, "session", None))
            return {
                "success": True,
                "user_id": user_id,
                "requires_email_confirmation": not bool(session),
            }
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    def create_pilot_brand(
        self,
        *,
        email: str,
        password: str,
        full_name: str,
        company_name: str,
        plan: str = "starter",
        email_confirm: bool = True,
        pilot_expires_at: str | None = None,
        max_searches_override: int | None = None,
        country: str | None = None,
        timezone_name: str | None = None,
        industry: str | None = None,
        preferred_currency: str | None = None,
        source_of_signup: str | None = "pilot",
    ) -> dict[str, Any]:
        try:
            normalized_plan = self._normalize_plan(plan)
            if max_searches_override is not None and int(max_searches_override) <= 0:
                raise ValueError("max_searches_override must be greater than 0")

            auth_response = self.admin.auth.admin.create_user(
                {
                    "email": email,
                    "password": password,
                    "email_confirm": email_confirm,
                    "user_metadata": {"full_name": full_name},
                }
            )
            user = self._to_dict(getattr(auth_response, "user", None))
            user_id = user.get("id")
            if not user_id:
                raise RuntimeError("Pilot user creation failed")

            brand_payload = {
                "id": user_id,
                "email": email,
                "full_name": full_name,
                "company_name": company_name,
                "plan": normalized_plan,
                "searches_used": 0,
                "billing_status": "inactive",
                "is_pilot": True,
                "pilot_status": "active",
                "pilot_expires_at": pilot_expires_at,
                "max_searches_override": max_searches_override,
                "country": self._clean_optional_str(country),
                "timezone": self._clean_optional_str(timezone_name),
                "industry": self._clean_optional_str(industry),
                "preferred_currency": self._clean_optional_str(preferred_currency),
                "source_of_signup": self._clean_optional_str(source_of_signup),
            }

            self.admin.table("brands").upsert(brand_payload, on_conflict="id").execute()

            created_brand = self.get_brand(user_id)
            return {
                "success": True,
                "user_id": user_id,
                "profile": created_brand,
                "access": self.get_access_summary(created_brand),
            }
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    def login_brand(self, email: str, password: str) -> dict[str, Any]:
        try:
            auth_response = self.public.auth.sign_in_with_password(
                {
                    "email": email,
                    "password": password,
                }
            )
            user = self._to_dict(getattr(auth_response, "user", None))
            session = getattr(auth_response, "session", None)
            user_id = user.get("id")
            if not user_id or session is None:
                raise RuntimeError("Login failed")

            profile = (
                self.admin.table("brands").select("*").eq("id", user_id).single().execute()
            )
            return {
                "success": True,
                "session": session,
                "profile": profile.data,
                "access": self.get_access_summary(profile.data),
            }
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    def verify_access_token(self, access_token: str) -> str:
        claims_response = self.public.auth.get_claims(access_token)
        claims = self._to_dict(claims_response)
        if isinstance(claims.get("claims"), dict):
            claims = claims["claims"]
        elif isinstance(claims.get("data"), dict) and isinstance(claims["data"].get("claims"), dict):
            claims = claims["data"]["claims"]

        user_id = claims.get("sub")
        if not user_id:
            raise ValueError("Invalid or expired token")
        return str(user_id)

    # ================================================================
    # BRAND PROFILE
    # ================================================================

    def get_brand(self, brand_id: str) -> dict[str, Any] | None:
        try:
            result = self.admin.table("brands").select("*").eq("id", brand_id).single().execute()
            return result.data
        except Exception:
            return None

    def set_billing_status(self, brand_id: str, billing_status: str) -> bool:
        billing_value = str(billing_status or "inactive")
        if billing_value not in VALID_BILLING_STATUSES:
            raise ValueError(f"Invalid billing_status: {billing_status}")
        try:
            self.admin.table("brands").update({"billing_status": billing_value}).eq("id", brand_id).execute()
            return True
        except Exception:
            return False

    def activate_paid_brand(self, brand_id: str, new_plan: str) -> bool:
        normalized_plan = self._normalize_plan(new_plan)
        try:
            self.admin.table("brands").update(
                {
                    "plan": normalized_plan,
                    "searches_used": 0,
                    "billing_status": "active",
                }
            ).eq("id", brand_id).execute()
            return True
        except Exception:
            return False

    def update_plan(self, brand_id: str, new_plan: str) -> bool:
        normalized_plan = self._normalize_plan(new_plan)
        try:
            self.admin.table("brands").update(
                {
                    "plan": normalized_plan,
                    "searches_used": 0,
                }
            ).eq("id", brand_id).execute()
            return True
        except Exception:
            return False

    def check_search_limit(self, brand_id: str) -> dict[str, Any]:
        brand = self.get_brand(brand_id)
        if not brand:
            return {"allowed": False, "error": "Brand not found"}

        access = self.get_access_summary(brand)
        if not access["has_access"]:
            return {
                "allowed": False,
                "error": access["reason"],
                "plan": brand.get("plan", "starter"),
                "used": int(brand.get("searches_used", 0)),
                "limit": self.get_brand_search_limit_from_profile(brand),
                "access": access,
            }

        plan = str(brand.get("plan", "starter"))
        used = int(brand.get("searches_used", 0))
        limit = self.get_brand_search_limit_from_profile(brand)
        return {
            "allowed": used < limit,
            "used": used,
            "limit": limit,
            "plan": plan,
            "access": access,
        }

    def increment_search_count(self, brand_id: str) -> None:
        brand = self.get_brand(brand_id)
        if not brand:
            return
        current_used = int(brand.get("searches_used", 0))
        self.admin.table("brands").update({"searches_used": current_used + 1}).eq("id", brand_id).execute()

    # ================================================================
    # SEARCH HISTORY
    # ================================================================

    # def log_search(self, brand_id: str, query: str, results: list[dict[str, Any]], plan: str) -> None:
    #     try:
    #         self.admin.table("search_history").insert(
    #             {
    #                 "brand_id": brand_id,
    #                 "query": query,
    #                 "results_count": len(results),
    #                 "result_ids": [r.get("video_id") or r.get("id") for r in results],
    #                 "plan_at_search": plan,
    #             }
    #         ).execute()
    #     except Exception:
    #         pass
    #
    # def get_search_history(self, brand_id: str, limit: int = 20) -> list[dict[str, Any]]:
    #     try:
    #         result = (
    #             self.admin.table("search_history")
    #             .select("*")
    #             .eq("brand_id", brand_id)
    #             .order("created_at", desc=True)
    #             .limit(limit)
    #             .execute()
    #         )
    #         return result.data
    #     except Exception:
    #         return []
    def log_search(self, brand_id: str, query: str, results: list[dict[str, Any]], plan: str) -> None:
        try:
            payload = {
                "brand_id": brand_id,
                "query": query,
                "results_count": len(results),
                "result_ids": [r.get("video_id") or r.get("id") for r in results],
                "plan_at_search": plan,
            }
            print("[History insert payload]", payload)

            self.admin.table("search_history").insert(payload).execute()
            print("[History ✓] Search logged")
        except Exception as exc:
            print("[History ✗] Insert failed:", exc)
            raise

    def get_search_history(self, brand_id: str, limit: int = 20) -> list[dict[str, Any]]:
        try:
            result = (
                self.admin.table("search_history")
                .select("*")
                .eq("brand_id", brand_id)
                .order("created_at", desc=True)
                .limit(limit)
                .execute()
            )
            print("[History fetch]", result.data)
            return result.data or []
        except Exception as exc:
            print("[History ✗] Fetch failed:", exc)
            raise

    # ================================================================
    # PAYMENTS
    # ================================================================

    def get_payment(self, reference: str) -> dict[str, Any] | None:
        try:
            result = (
                self.admin.table("payments")
                .select("*")
                .eq("paystack_reference", reference)
                .limit(1)
                .execute()
            )
            return self._first_or_none(result)
        except Exception:
            return None

    def log_payment(
        self, brand_id: str, reference: str, amount: int, plan: str, status: str = "pending"
    ) -> dict[str, Any] | None:
        existing = self.get_payment(reference)
        if existing:
            return existing
        try:
            result = self.admin.table("payments").insert(
                {
                    "brand_id": brand_id,
                    "paystack_reference": reference,
                    "amount": amount,
                    "plan": self._normalize_plan(plan),
                    "status": status,
                }
            ).execute()
            return self._first_or_none(result)
        except Exception:
            return None

    def set_payment_status(self, reference: str, status: str) -> bool:
        payload = {"status": status}
        if status == "success":
            payload["paid_at"] = datetime.now(timezone.utc).isoformat()
        try:
            self.admin.table("payments").update(payload).eq("paystack_reference", reference).execute()
            return True
        except Exception:
            if "paid_at" in payload:
                try:
                    self.admin.table("payments").update({"status": status}).eq(
                        "paystack_reference", reference
                    ).execute()
                    return True
                except Exception:
                    return False
            return False

    def apply_verified_payment(
        self,
        *,
        reference: str,
        verified_amount: int | None = None,
        paystack_status: str = "success",
    ) -> dict[str, Any]:
        payment = self.get_payment(reference)
        if not payment:
            raise RuntimeError("Payment record not found")

        if payment.get("status") == "success":
            brand = self.get_brand(payment["brand_id"])
            return {
                "brand": brand,
                "payment": payment,
                "already_processed": True,
                "access": self.get_access_summary(brand),
            }

        if paystack_status != "success":
            self.set_payment_status(reference, paystack_status)
            raise RuntimeError(f"Transaction is not successful: {paystack_status}")

        expected_amount = payment.get("amount")
        if verified_amount is not None and expected_amount is not None and int(verified_amount) != int(expected_amount):
            raise RuntimeError("Verified payment amount does not match logged amount")

        self.set_payment_status(reference, "success")
        updated = self.get_payment(reference) or payment
        self.activate_paid_brand(payment["brand_id"], payment["plan"])
        brand = self.get_brand(payment["brand_id"])
        return {
            "brand": brand,
            "payment": updated,
            "already_processed": False,
            "access": self.get_access_summary(brand),
        }

    # ================================================================
    # CREATOR STORAGE
    # ================================================================

    # def save_creator(self, video_id: str, metadata: dict[str, Any], embedded: dict[str, Any]) -> str | None:
    #     try:
    #         creator_row = {
    #             "video_id": video_id,
    #             "url": f"https://www.youtube.com/watch?v={video_id}",
    #             "title": metadata.get("title", ""),
    #             "channel": metadata.get("channel", ""),
    #             "description": metadata.get("description", ""),
    #             "tags": metadata.get("tags", []),
    #             "duration": metadata.get("duration", ""),
    #             "content_source": embedded.get("source", ""),
    #             "rewritten_text": embedded.get("rewritten", ""),
    #         }
    #         result = self.admin.table("creators").upsert(creator_row, on_conflict="video_id").execute()
    #         creator = self._first_or_none(result)
    #         if not creator:
    #             return None
    #         creator_id = creator["id"]
    #
    #         self.admin.table("creator_vectors").upsert(
    #             {
    #                 "creator_id": creator_id,
    #                 "video_id": video_id,
    #                 "embedding": embedded.get("vector"),
    #             },
    #             on_conflict="video_id",
    #         ).execute()
    #         return str(creator_id)
    #     except Exception:
    #         return None
    #
    # def save_all(self, embedded_creators: dict[str, Any], metadata_map: dict[str, Any]) -> dict[str, list[str]]:
    #     saved: list[str] = []
    #     failed: list[str] = []
    #     for vid_id, embedded in embedded_creators.items():
    #         metadata = metadata_map.get(vid_id, {})
    #         result = self.save_creator(vid_id, metadata, embedded)
    #         (saved if result else failed).append(vid_id)
    #     return {"saved": saved, "failed": failed}
    def save_creator(self, video_id: str, metadata: dict[str, Any], embedded: dict[str, Any]) -> dict[str, Any]:
        try:
            creator_row = {
                "video_id": video_id,
                "url": f"https://www.youtube.com/watch?v={video_id}",
                "title": metadata.get("title", ""),
                "channel": metadata.get("channel", ""),
                "description": metadata.get("description", ""),
                "tags": metadata.get("tags", []),
                "duration": metadata.get("duration", ""),
                "content_source": embedded.get("source", ""),
                "rewritten_text": embedded.get("rewritten", ""),
            }

            creator_result = self.admin.table("creators").upsert(
                creator_row,
                on_conflict="video_id",
                returning="representation",
            ).execute()

            creator = self._first_or_none(creator_result)
            if not creator:
                return {
                    "ok": False,
                    "stage": "creators",
                    "video_id": video_id,
                    "error": "Creators upsert returned no row",
                }

            creator_id = creator["id"]
            vector = embedded.get("vector")

            if not isinstance(vector, list) or not vector:
                return {
                    "ok": False,
                    "stage": "creator_vectors",
                    "video_id": video_id,
                    "error": "Missing embedding vector",
                }

            vector_result = self.admin.table("creator_vectors").upsert(
                {
                    "creator_id": creator_id,
                    "video_id": video_id,
                    "embedding": vector,
                },
                on_conflict="video_id",
                returning="representation",
            ).execute()

            vector_row = self._first_or_none(vector_result)
            if not vector_row:
                return {
                    "ok": False,
                    "stage": "creator_vectors",
                    "video_id": video_id,
                    "error": "Creator vector upsert returned no row",
                }

            return {
                "ok": True,
                "video_id": video_id,
                "creator_id": str(creator_id),
            }

        except Exception as exc:
            return {
                "ok": False,
                "stage": "exception",
                "video_id": video_id,
                "error": str(exc),
            }

    def save_all(self, embedded_creators: dict[str, Any], metadata_map: dict[str, Any]) -> dict[str, Any]:
        saved: list[str] = []
        failed: list[dict[str, Any]] = []

        for vid_id, embedded in embedded_creators.items():
            metadata = metadata_map.get(vid_id, {})
            result = self.save_creator(vid_id, metadata, embedded)

            if result.get("ok"):
                saved.append(vid_id)
            else:
                failed.append(result)

        return {
            "saved": saved,
            "failed": failed,
        }

    def search_creators(
        self, query_vector: list[float], threshold: float = 0.75, limit: int = 10
    ) -> list[dict[str, Any]]:
        try:
            result = self.admin.rpc(
                "search_creators",
                {
                    "query_embedding": query_vector,
                    "match_threshold": threshold,
                    "match_count": limit,
                },
            ).execute()
            return result.data or []
        except Exception:
            return []

    # def brand_search(
    #     self,
    #     *,
    #     brand_id: str,
    #     query: str,
    #     embedder: Any,
    #     threshold: float = 0.75,
    #     limit: int = 10,
    # ) -> dict[str, Any]:
    #     limit_check = self.check_search_limit(brand_id)
    #     if not limit_check.get("allowed"):
    #         return {
    #             "success": False,
    #             "error": limit_check.get("error")
    #             or f"Search limit reached ({limit_check.get('used', 0)}/{limit_check.get('limit', 0)})",
    #             "access": limit_check.get("access"),
    #         }
    #
    #     query_vector = embedder.embed_query(query)
    #     matches = self.search_creators(query_vector, threshold=threshold, limit=limit)
    #     self.log_search(brand_id, query, matches, limit_check["plan"])
    #     self.increment_search_count(brand_id)
    #
    #     return {
    #         "success": True,
    #         "matches": matches,
    #         "used": limit_check["used"] + 1,
    #         "limit": limit_check["limit"],
    #         "plan": limit_check["plan"],
    #         "access": limit_check.get("access"),
    #     }
    def brand_search(
            self,
            *,
            brand_id: str,
            query: str,
            embedder: Any,
            threshold: float = 0.75,
            limit: int = 10,
    ) -> dict[str, Any]:
        limit_check = self.check_search_limit(brand_id)
        if not limit_check.get("allowed"):
            return {
                "success": False,
                "error": limit_check.get("error")
                         or f"Search limit reached ({limit_check.get('used', 0)}/{limit_check.get('limit', 0)})",
                "access": limit_check.get("access"),
            }

        query_vector = embedder.embed_query(query)
        matches = self.search_creators(query_vector, threshold=threshold, limit=limit)

        # log first
        self.log_search(brand_id, query, matches, limit_check["plan"])

        # only count a search if it actually returned results
        if matches:
            self.increment_search_count(brand_id)
            used = limit_check["used"] + 1
        else:
            used = limit_check["used"]

        return {
            "success": True,
            "matches": matches,
            "used": used,
            "limit": limit_check["limit"],
            "plan": limit_check["plan"],
            "access": limit_check.get("access"),
        }
