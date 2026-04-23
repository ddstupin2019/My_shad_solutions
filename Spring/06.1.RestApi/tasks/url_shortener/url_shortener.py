from uuid import uuid4
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException, status
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, field_validator

app = FastAPI()

key_to_url: dict[str, str] = {}
url_to_key: dict[str, str] = {}


class ToShort(BaseModel):
    url: str

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        parsed = urlparse(value)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ValueError("invalid url")
        return value


class Shorted(BaseModel):
    url: str
    key: str


@app.post(
    "/shorten",
    response_model=Shorted,
    status_code=status.HTTP_201_CREATED,
)
def short_url(link: ToShort) -> Shorted:
    if link.url in url_to_key:
        key = url_to_key[link.url]
        return Shorted(url=link.url, key=key)

    key = uuid4().hex[:6]
    while key in key_to_url:
        key = uuid4().hex[:6]

    key_to_url[key] = link.url
    url_to_key[link.url] = key

    return Shorted(url=link.url, key=key)


@app.get(
    "/go/{key}",
    status_code=status.HTTP_307_TEMPORARY_REDIRECT,
    responses={
        307: {
            "description": "Successful Response",
            "content": {
                "application/json": {
                    "schema": {
                        "title": "Response Redirect To Url Go  Key  Get"
                    }
                }
            },
        }
    },
)
def redirect_to_url(key: str):
    if key not in key_to_url:
        raise HTTPException(status_code=404, detail="Not found")

    return RedirectResponse(
        url=key_to_url[key],
        status_code=status.HTTP_307_TEMPORARY_REDIRECT,
    )
