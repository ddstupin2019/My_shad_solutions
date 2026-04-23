import secrets
import string
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Header, Query
from pydantic import BaseModel

app = FastAPI()

users_token = {}
tracks = {}
track_id = 1


class UserRegistration(BaseModel):
    name: str
    age: int


class Track(BaseModel):
    name: str
    artist: str
    year: int | None = None
    genres: list[str] | None = []


def generate_token(length: int = 40) -> str:
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))


def check_token(x_token: Optional[str] = Header(default=None)):
    if x_token is None:
        raise HTTPException(status_code=401, detail="Missing token")
    if x_token not in users_token:
        raise HTTPException(status_code=401, detail="Incorrect token")
    return x_token


def check_track_id(track_id: int):
    if track_id not in tracks:
        raise HTTPException(status_code=404, detail="Invalid track_id")
    return track_id


@app.post("/api/v1/registration/register_user")
def register_user(user: UserRegistration):
    token = generate_token()
    users_token[token] = user
    return {"token": token}


@app.post("/api/v1/tracks/add_track", status_code=201)
def add_track(track: Track, token: str = Depends(check_token)):
    global track_id
    tracks[track_id] = {
        "name": track.name,
        "artist": track.artist,
        "year": track.year,
        "genres": track.genres,
    }
    track_id += 1

    return {"track_id": track_id - 1}


@app.get("/api/v1/tracks/all")
def get_tracks(token: str = Depends(check_token)):
    return list(tracks.values())


@app.get("/api/v1/tracks/search")
def search_tracks(name: Optional[str] = Query(default=None),
                  artist: Optional[str] = Query(default=None),
                  token: str = Depends(check_token)):
    if not name and not artist:
        raise HTTPException(
            status_code=422, detail="You should specify at least one search argument")

    rez = []
    for k, v in tracks.items():
        if (not name or v['name'] == name) and (not artist or v['artist'] == artist):
            rez.append(k)

    return {"track_ids": rez}


@app.delete("/api/v1/tracks/{track_id}")
def del_track(track_id: int, token: str = Depends(check_token)):
    check_track_id(track_id)
    del tracks[track_id]
    return {"status": "track removed"}


@app.get("/api/v1/tracks/{track_id}")
def get_track(track_id: int, token: str = Depends(check_token)):
    check_track_id(track_id)
    return {"name": tracks[track_id]['name'], "artist": tracks[track_id]['artist']}
