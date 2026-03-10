import requests

from app.core.settings import Settings


def rest_headers(settings: Settings) -> dict[str, str]:
    return {
        "apikey": settings.supabase_service_role,
        "Authorization": f"Bearer {settings.supabase_service_role}",
    }


def get_json(
    settings: Settings,
    table: str,
    select: str,
    params: dict,
    limit: int | None = None,
) -> list[dict]:
    url = f"{settings.supabase_url}/rest/v1/{table}"
    query_params = {"select": select, **params}
    if limit is not None:
        query_params["limit"] = str(limit)
    response = requests.get(
        url,
        headers=rest_headers(settings),
        params=query_params,
        timeout=20,
    )
    if not response.ok:
        raise RuntimeError(
            f"supabase_{table}_{response.status_code}:{response.text[:200]}"
        )
    return response.json() or []
