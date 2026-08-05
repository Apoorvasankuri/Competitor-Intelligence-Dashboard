# cmie_client.py — 1 fix, based on CMIE's real (documented) response format

The real CMIE response wraps errno/errmsg inside a "meta" object on both
success and error responses:
    Success: {"meta": {"errno": 0, "errmsg": "Success", ...}, "data": {...}}
    Error:   {"meta": {"errno": -52, "errmsg": "Invalid parameter...", ...}}

The current code checks for "errno"/"errmsg" at the TOP level of the
response, which never matches this shape -- meaning real CMIE errors
currently pass through silently instead of raising CmieApiError.

## Find this block in call_cmie_api():

```python
    # Errors can come back as a dict with errno/errmsg, or as a list containing
    # such a dict as its first element. Check both shapes defensively.
    error_container = None
    if isinstance(data, dict):
        error_container = data
    elif isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
        if "errno" in data[0] or "errmsg" in data[0]:
            error_container = data[0]

    if error_container is not None:
        errno = error_container.get("errno")
        errmsg = error_container.get("errmsg")
        if errno or errmsg:
            raise CmieApiError(f"CMIE API error (errno={errno}): {errmsg}")

    return data
```

## Replace it with:

```python
    # CMIE's real response format nests errno/errmsg inside "meta" on both
    # success (errno=0) and error responses (errno=negative). Only raise
    # when errno is present and non-zero -- errno=0 with errmsg="Success"
    # must NOT raise.
    meta = data.get("meta") if isinstance(data, dict) else None
    if isinstance(meta, dict):
        errno = meta.get("errno")
        errmsg = meta.get("errmsg")
        if errno not in (0, "0", None):
            raise CmieApiError(f"CMIE API error (errno={errno}): {errmsg}")

    # Defensive fallback for any response shape that doesn't use "meta"
    # (kept in case a future/other reporttype responds differently).
    elif isinstance(data, dict) and ("errno" in data or "errmsg" in data):
        errno = data.get("errno")
        if errno not in (0, "0", None):
            raise CmieApiError(f"CMIE API error (errno={errno}): {data.get('errmsg')}")

    return data
```

That's the only change needed in this file.
