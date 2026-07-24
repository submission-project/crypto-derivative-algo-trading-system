from fastapi import APIRouter
from api_server.runtime import is_app_ready

router = APIRouter(tags=["Health"])

@router.get("/health")
async def health_check():
    return {"status": "ok"}

@router.get("/ready")
async def ready():
    if not is_app_ready():
        return {
            "status": "not_ready",
        }

    return {
        "status": "ready",
    }