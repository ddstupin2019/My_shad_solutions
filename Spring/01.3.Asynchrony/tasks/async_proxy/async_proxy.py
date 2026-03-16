import aiohttp
from aiohttp import web
from yarl import URL


async def proxy_handler(request: web.Request) -> web.Response:
    """
    Check request contains http url in query args:
        /fetch?url=http%3A%2F%2Fexample.com%2F
    and trying to fetch it and return body with http status.
    If url passed without scheme or is invalid raise 400 Bad request.
    On failure raise 502 Bad gateway.
    :param request: aiohttp.web.Request to handle
    :return: aiohttp.web.Response
    """
    raw = request.query.get("url")  # pyrefly: ignore
    if not raw:
        raise web.HTTPBadRequest(text="No url to fetch")

    url = URL(raw)

    if not url.is_absolute():
        raise web.HTTPBadRequest(text="Empty url scheme")
    if url.scheme not in ("http", "https"):
        raise web.HTTPBadRequest(text="Bad url scheme: ftp")

    session: aiohttp.ClientSession = request.app["session"]

    async with session.get(url) as resp:
        body = await resp.read()
        return web.Response(status=resp.status, body=body)


async def setup_application(app: web.Application) -> None:
    """
    Setup application routes and aiohttp session for fetching
    :param app: app to apply settings with
    """
    app["session"] = aiohttp.ClientSession()
    app.router.add_get("/fetch", proxy_handler)


async def teardown_application(app: web.Application) -> None:
    """
    Application with aiohttp session for tearing down
    :param app: app for tearing down
    """
    if "session" in app:
        await app["session"].close()
