"""Small helpers for issuing read-oriented REST calls to Supabase."""

import requests

from app.core.settings import Settings


def rest_headers(settings: Settings) -> dict[str, str]:
    """Build the authorization headers required for Supabase REST requests.

    Args:
        settings (Settings): Runtime settings containing the service-role token.

    Returns:
        dict[str, str]: Header mapping with API key and bearer authorization.
    """
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
    """Fetch JSON rows from a Supabase table through the REST API.

    Args:
        settings (Settings): Runtime settings with Supabase connection details.
        table (str): Table or view name to query.
        select (str): Supabase `select` projection string.
        params (dict): Additional filter and query parameters.
        limit (int | None): Optional maximum number of rows to return.

    Returns:
        list[dict]: Decoded JSON rows returned by Supabase.

    Raises:
        RuntimeError: Raised when Supabase returns a non-success response.
    """
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
