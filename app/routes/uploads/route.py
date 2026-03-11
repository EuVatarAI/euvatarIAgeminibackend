"""FastAPI routes for upload operations."""

from fastapi import APIRouter

from app.routes.uploads.controller import UploadsController
from app.routes.uploads.dtos import ConfirmUploadRequest, CreateSignedUploadRequest

router = APIRouter()
controller = UploadsController()


@router.post("/uploads/signed-url")
async def create_signed_url(request: CreateSignedUploadRequest):
    """Create a signed upload URL for a client-side file upload.

    Args:
        request (CreateSignedUploadRequest): Request body describing the asset to upload.

    Returns:
        dict | JSONResponse: Response produced by the uploads controller.
    """
    return await controller.create_signed_url(request)


@router.post("/uploads/confirm")
async def confirm_upload(request: ConfirmUploadRequest):
    """Confirm that a previously signed upload has completed successfully.

    Args:
        request (ConfirmUploadRequest): Request body describing the completed upload.

    Returns:
        dict | JSONResponse: Response produced by the uploads controller.
    """
    return await controller.confirm_upload(request)
