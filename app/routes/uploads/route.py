from fastapi import APIRouter

from app.routes.uploads.controller import UploadsController
from app.routes.uploads.dtos import ConfirmUploadRequest, CreateSignedUploadRequest

router = APIRouter()
controller = UploadsController()


@router.post("/uploads/signed-url")
async def create_signed_url(request: CreateSignedUploadRequest):
    return await controller.create_signed_url(request)


@router.post("/uploads/confirm")
async def confirm_upload(request: ConfirmUploadRequest):
    return await controller.confirm_upload(request)
