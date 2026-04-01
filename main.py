from __future__ import annotations

import os
import dotenv
import secrets
import uuid
from datetime import datetime
from typing import Optional
from datetime import datetime, timezone, timedelta
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, EmailStr, Field
from fastapi.middleware.cors import CORSMiddleware

from embedding import EmbeddingService
from paystack_service import PaystackService
from supabaseserver import PLAN_LIMITS, StorageService
from yt import YOUTUBE_SEARCH

app = FastAPI(title="Inflex API", version="1.2.0")
origins = [
    "https://inflex-frontend-eijmsbzop-hexoramthehackers-projects.vercel.app",
    "https://www.yourcustomdomain.com",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_origin_regex=r"https://.*\.vercel\.app",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
storage = StorageService()
embedder = EmbeddingService()
paystack = PaystackService()
ENV = dotenv.dotenv_values(".env")
ADMIN_INVITE_SECRET = ENV.get("INTERNAL_ADMIN_SECRET", "")
security = HTTPBearer()
try:
    youtube = YOUTUBE_SEARCH()
except Exception:
    youtube = None


class RegisterRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8)
    full_name: str
    company_name: str


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class SearchRequest(BaseModel):
    query: str
    threshold: float = Field(default=0.75, ge=0.0, le=1.0)
    limit: int = Field(default=10, ge=1, le=20)


class BillingInitializeRequest(BaseModel):
    plan: str
    amount: int = Field(gt=0, description="Amount in kobo")
    callback_url: Optional[str] = None
    plan_code: Optional[str] = None


class PaymentVerifyRequest(BaseModel):
    reference: str


class PilotInviteRequest(BaseModel):
    email: EmailStr
    full_name: str
    company_name: str
    plan: str = Field(default="starter")
    temporary_password: Optional[str] = Field(default=None, min_length=8)
    email_confirm: bool = True
    pilot_expires_at: Optional[datetime] = None
    max_searches_override: Optional[int] = Field(default=None, gt=0)
    country: Optional[str] = None
    timezone: Optional[str] = None
    industry: Optional[str] = None
    preferred_currency: Optional[str] = None
    source_of_signup: Optional[str] = "pilot"


# async def get_current_brand(authorization: str = Header(...)) -> str:
#     try:
#         token = authorization.replace("Bearer ", "").strip()
#         return storage.verify_access_token(token)
#     except Exception:
#         raise HTTPException(status_code=401, detail="Invalid or expired token")
async def get_current_brand(credentials: HTTPAuthorizationCredentials = Depends(security),) -> str:
    try:
        token = credentials.credentials
        return storage.verify_access_token(token)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired token")


async def require_admin_secret(x_admin_secret: str = Depends(security)) -> None:
    if not ADMIN_INVITE_SECRET:
        raise HTTPException(status_code=500, detail="INTERNAL_ADMIN_SECRET is not configured")
    if x_admin_secret != ADMIN_INVITE_SECRET:
        raise HTTPException(status_code=403, detail="Invalid admin secret")


@app.get("/")
async def root() -> dict[str, str | bool]:
    return {
        "message": "Inflex API is running",
        "youtube_fallback_enabled": youtube is not None,
        "paid_only_access": True,
    }


@app.post("/auth/register")
async def register(body: RegisterRequest) -> dict:
    result = storage.register_brand(
        email=body.email,
        password=body.password,
        full_name=body.full_name,
        company_name=body.company_name,
    )
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["error"])

    return {
        "message": "Account created successfully",
        "user_id": result["user_id"],
        "plan": "starter",
        "requires_email_confirmation": result.get("requires_email_confirmation", False),
        "billing_status": "inactive",
        "access": {
            "has_access": False,
            "reason": "Complete payment or receive pilot access to start using the product",
        },
    }


@app.post("/auth/login")
async def login(body: LoginRequest) -> dict:
    result = storage.login_brand(email=body.email, password=body.password)
    if not result["success"]:
        raise HTTPException(status_code=401, detail=result["error"])

    return {
        "message": "Login successful",
        "token": result["session"].access_token,
        "profile": result["profile"],
        "access": result.get("access"),
    }


@app.get("/auth/me")
async def me(brand_id: str = Depends(get_current_brand)) -> dict:
    profile = storage.get_brand(brand_id)
    if not profile:
        raise HTTPException(status_code=404, detail="Brand profile not found")
    limit_check = storage.check_search_limit(brand_id)
    return {
        "brand_id": brand_id,
        "profile": profile,
        "usage": limit_check,
        "access": storage.get_access_summary(profile),
    }


# @app.post("/pilot/invite")
@app.post("/pilot/invite")
async def invite_pilot_brand(body: PilotInviteRequest, _: None = Depends(require_admin_secret)) -> dict:
    if body.plan not in PLAN_LIMITS:
        raise HTTPException(status_code=400, detail=f"Plan must be one of: {', '.join(PLAN_LIMITS.keys())}")

    temporary_password = body.temporary_password or secrets.token_urlsafe(12)

    now = datetime.now(timezone.utc)

    if body.pilot_expires_at is None:
        expiry_dt = now + timedelta(days=30)
    else:
        expiry_dt = body.pilot_expires_at
        if expiry_dt.tzinfo is None:
            expiry_dt = expiry_dt.replace(tzinfo=timezone.utc)
        else:
            expiry_dt = expiry_dt.astimezone(timezone.utc)

        if expiry_dt <= now:
            raise HTTPException(
                status_code=400,
                detail="pilot_expires_at must be in the future",
            )

    pilot_expires_at = expiry_dt.isoformat()

    result = storage.create_pilot_brand(
        email=body.email,
        password=temporary_password,
        full_name=body.full_name,
        company_name=body.company_name,
        plan=body.plan,
        email_confirm=body.email_confirm,
        pilot_expires_at=pilot_expires_at,
        max_searches_override=body.max_searches_override,
        country=body.country,
        timezone_name=body.timezone,
        industry=body.industry,
        preferred_currency=body.preferred_currency,
        source_of_signup=body.source_of_signup,
    )
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["error"])

    return {
        "message": "Pilot brand created successfully",
        "user_id": result["user_id"],
        "temporary_password": temporary_password,
        "profile": result["profile"],
        "access": result["access"],
    }
# def invite_pilot_brand(body: PilotInviteRequest, _: None = Depends(require_admin_secret)) -> dict:
#     if body.plan not in PLAN_LIMITS:
#         raise HTTPException(status_code=400, detail=f"Plan must be one of: {', '.join(PLAN_LIMITS.keys())}")
#
#     temporary_password = body.temporary_password or secrets.token_urlsafe(12)
#     pilot_expires_at = body.pilot_expires_at.isoformat() if body.pilot_expires_at else None
#
#     result = storage.create_pilot_brand(
#         email=body.email,
#         password=temporary_password,
#         full_name=body.full_name,
#         company_name=body.company_name,
#         plan=body.plan,
#         email_confirm=body.email_confirm,
#         pilot_expires_at=pilot_expires_at,
#         max_searches_override=body.max_searches_override,
#         country=body.country,
#         timezone_name=body.timezone,
#         industry=body.industry,
#         preferred_currency=body.preferred_currency,
#         source_of_signup=body.source_of_signup,
#     )
#     if not result["success"]:
#         raise HTTPException(status_code=400, detail=result["error"])
#
#     return {
#         "message": "Pilot brand created successfully",
#         "user_id": result["user_id"],
#         "temporary_password": temporary_password,
#         "profile": result["profile"],
#         "access": result["access"],
#     }


# @app.post("/search")
# def search(body: SearchRequest, brand_id: str = Depends(get_current_brand)) -> dict:
#     if not body.query.strip():
#         raise HTTPException(status_code=400, detail="Search query cannot be empty")
#
#     result = storage.brand_search(
#         brand_id=brand_id,
#         query=body.query,
#         embedder=embedder,
#         threshold=body.threshold,
#         limit=body.limit,
#     )
#     if not result["success"]:
#         raise HTTPException(status_code=403, detail={
#             "message": result["error"],
#             "access": result.get("access"),
#         })
#
#     return {
#         "query": body.query,
#         "matches": result["matches"],
#         "results_count": len(result["matches"]),
#         "searches_used": result["used"],
#         "searches_left": result["limit"] - result["used"],
#         "plan": result["plan"],
#         "access": result.get("access"),
#     }

@app.post("/search")
async def search(body: SearchRequest, brand_id: str = Depends(get_current_brand)) -> dict:
    if not body.query.strip():
        raise HTTPException(status_code=400, detail="Search query cannot be empty")

    # Step 1: internal search first
    result = storage.brand_search(
        brand_id=brand_id,
        query=body.query,
        embedder=embedder,
        threshold=body.threshold,
        limit=body.limit,
    )

    if not result["success"]:
        raise HTTPException(
            status_code=403,
            detail={
                "message": result["error"],
                "access": result.get("access"),
            },
        )

    matches = result["matches"]
    youtube_fallback_used = False
    growth = {
        "attempted": False,
        "saved": 0,
        "failed": 0,
        "indexed_candidates": 0,
        "error": None,
    }

    # Step 2: fallback to YouTube if results are weak or empty
    # if (not matches or len(matches) < body.limit) and youtube is not None:
    #     youtube_fallback_used = True
    #     growth["attempted"] = True
    #
    #     try:
    #         yt_results = youtube.search(body.query)
    #         metadata_map = {item["video_id"]: item for item in yt_results}
    #
    #         embedding_texts = youtube.get_embedding_text(yt_results)
    #         embedded_creators = embedder.embed_creators(embedding_texts)
    #
    #         growth["indexed_candidates"] = len(embedded_creators)
    #
    #         save_result = storage.save_all(embedded_creators, metadata_map)
    #         growth["saved"] = len(save_result["saved"])
    #         growth["failed"] = len(save_result["failed"])
    if (not matches or len(matches) < body.limit) and youtube is not None:
        youtube_fallback_used = True
        growth["attempted"] = True

        try:
            yt_results = youtube.search(body.query)

            # Build proper maps using yt.py helpers
            metadata_map = youtube.get_metadata(yt_results)
            embedding_texts = youtube.get_embedding_text(yt_results)

            embedded_creators = embedder.embed_creators(embedding_texts)

            growth["indexed_candidates"] = len(embedded_creators)

            save_result = storage.save_all(embedded_creators, metadata_map)
            growth["saved"] = len(save_result["saved"])
            growth["failed"] = len(save_result["failed"])
            growth["failed_details"] = save_result["failed"]


            # Re-run internal search after indexing
            query_vector = embedder.embed_query(body.query)
            matches = storage.search_creators(
                query_vector,
                threshold=body.threshold,
                limit=body.limit,
            )

        except Exception as exc:
            growth["error"] = str(exc)

            # Step 3: re-run internal search after indexing
            query_vector = embedder.embed_query(body.query)
            matches = storage.search_creators(
                query_vector,
                threshold=body.threshold,
                limit=body.limit,
            )

        except Exception as exc:
            growth["error"] = str(exc)

    return {
        "query": body.query,
        "matches": matches,
        "results_count": len(matches),
        "searches_used": result["used"],
        "searches_left": result["limit"] - result["used"],
        "plan": result["plan"],
        "access": result.get("access"),
        "youtube_fallback_used": youtube_fallback_used,
        "growth": growth,
    }
@app.get("/search/history")
async def search_history(limit: int = 20, brand_id: str = Depends(get_current_brand)) -> dict:
    history = storage.get_search_history(brand_id, limit=limit)
    return {
        "brand_id": brand_id,
        "count": len(history),
        "history": history,
    }


@app.post("/billing/initialize")
async def initialize_payment(body: BillingInitializeRequest, brand_id: str = Depends(get_current_brand)) -> dict:
    brand = storage.get_brand(brand_id)
    if not brand:
        raise HTTPException(status_code=404, detail="Brand profile not found")
    if body.plan not in PLAN_LIMITS:
        raise HTTPException(status_code=400, detail=f"Plan must be one of: {', '.join(PLAN_LIMITS.keys())}")

    reference = f"inflex_{uuid.uuid4().hex}"
    storage.log_payment(
        brand_id=brand_id,
        reference=reference,
        amount=body.amount,
        plan=body.plan,
        status="pending",
    )

    metadata = {
        "brand_id": brand_id,
        "plan": body.plan,
    }
    response = paystack.initialize_transaction(
        email=brand["email"],
        amount=body.amount,
        reference=reference,
        callback_url=body.callback_url,
        plan_code=body.plan_code,
        metadata=metadata,
    )
    data = response["data"]
    return {
        "message": response.get("message", "Authorization URL created"),
        "authorization_url": data["authorization_url"],
        "access_code": data["access_code"],
        "reference": data["reference"],
    }


@app.get("/billing/verify/{reference}")
async def verify_payment(reference: str, brand_id: str = Depends(get_current_brand)) -> dict:
    payment = storage.get_payment(reference)
    if not payment or payment.get("brand_id") != brand_id:
        raise HTTPException(status_code=404, detail="Payment reference not found for this user")

    verification = paystack.verify_transaction(reference)
    data = verification.get("data", {})
    status = data.get("status")
    if status != "success":
        storage.set_payment_status(reference, str(status or "pending"))
        raise HTTPException(status_code=400, detail=f"Payment is not successful: {status}")

    applied = storage.apply_verified_payment(
        reference=reference,
        verified_amount=data.get("amount"),
        paystack_status=str(status),
    )
    brand = applied["brand"] or storage.get_brand(brand_id)
    return {
        "message": "Payment verified successfully",
        "reference": reference,
        "plan": brand["plan"] if brand else None,
        "billing_status": brand.get("billing_status") if brand else None,
        "access": applied.get("access"),
        "already_processed": applied["already_processed"],
    }


@app.post("/billing/webhook")
async def paystack_webhook(request: Request) -> dict:
    raw_body = await request.body()
    signature = request.headers.get("x-paystack-signature")
    if not paystack.validate_webhook_signature(raw_body, signature):
        raise HTTPException(status_code=401, detail="Invalid Paystack signature")

    event = paystack.parse_event(raw_body)
    event_name = event.get("event")
    data = event.get("data", {})

    if event_name == "charge.success":
        reference = data.get("reference")
        if reference:
            try:
                storage.apply_verified_payment(
                    reference=reference,
                    verified_amount=data.get("amount"),
                    paystack_status="success",
                )
            except Exception:
                pass

    return {"received": True}


@app.get("/billing/subscription")
async def billing_subscription(brand_id: str = Depends(get_current_brand)) -> dict:
    profile = storage.get_brand(brand_id)
    if not profile:
        raise HTTPException(status_code=404, detail="Brand profile not found")
    limit_check = storage.check_search_limit(brand_id)
    access = storage.get_access_summary(profile)
    return {
        "plan": profile.get("plan"),
        "billing_status": profile.get("billing_status", "inactive"),
        "searches_used": limit_check.get("used", 0),
        "searches_left": max(limit_check.get("limit", 0) - limit_check.get("used", 0), 0),
        "searches_limit": limit_check.get("limit", 0),
        "company": profile.get("company_name"),
        "is_pilot": profile.get("is_pilot", False),
        "pilot_status": profile.get("pilot_status"),
        "pilot_expires_at": profile.get("pilot_expires_at"),
        "access": access,
    }


@app.get("/plan")
async def get_plan(brand_id: str = Depends(get_current_brand)) -> dict:
    return billing_subscription(brand_id)
