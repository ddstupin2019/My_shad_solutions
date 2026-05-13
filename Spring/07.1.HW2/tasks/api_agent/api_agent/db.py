import sqlite3
import uuid
from datetime import datetime, timezone
import os
from pathlib import Path
from typing import Any

DB_PATH = Path(os.getenv("DB_PATH", Path(__file__).resolve().parent.parent / "app.db"))


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    return db


def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return dict(row)


async def init_db() -> None:
    with _connect() as db:
        db.execute(
            """
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            login TEXT UNIQUE NOT NULL,
            password_hash TEXT,
            created_at TEXT NOT NULL
        )
        """
        )
        db.execute(
            """
        CREATE TABLE IF NOT EXISTS llm_configs (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            name TEXT NOT NULL,
            base_url TEXT NOT NULL,
            api_key TEXT NOT NULL,
            model TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
        """
        )
        db.execute(
            """
        CREATE TABLE IF NOT EXISTS chats (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            llm_config_id TEXT NOT NULL,
            title TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (llm_config_id) REFERENCES llm_configs(id)
        )
        """
        )
        db.execute(
            """
        CREATE TABLE IF NOT EXISTS messages (
            id TEXT PRIMARY KEY,
            chat_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (chat_id) REFERENCES chats(id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
        """
        )
        db.execute(
            """
        CREATE TABLE IF NOT EXISTS mcp_configs (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            name TEXT NOT NULL,
            url TEXT NOT NULL,
            token TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
        """
        )
        db.execute(
            """
        CREATE TABLE IF NOT EXISTS chat_mcp_configs (
            chat_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            mcp_config_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (chat_id, mcp_config_id),
            FOREIGN KEY (chat_id) REFERENCES chats(id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
        """
        )
        db.commit()


async def create_user(login: str, password_hash: str) -> str:
    user_id = str(uuid.uuid4())
    with _connect() as db:
        db.execute(
            "INSERT INTO users VALUES (?, ?, ?, ?)",
            (
                user_id,
                login,
                password_hash,
                _now(),
            ),
        )
        db.commit()
    return user_id


async def get_user_by_login(login: str):
    with _connect() as db:
        cursor = db.execute(
            "SELECT id, login, password_hash FROM users WHERE login = ?", (login,)
        )
        return cursor.fetchone()


async def get_user_by_id(user_id: str):
    with _connect() as db:
        cursor = db.execute("SELECT id, login FROM users WHERE id = ?", (user_id,))
        return cursor.fetchone()


async def create_llm_config(
    user_id: str,
    name: str,
    base_url: str,
    api_key: str,
    model: str,
) -> dict[str, Any]:
    config_id = str(uuid.uuid4())
    created_at = _now()
    with _connect() as db:
        db.execute(
            """
            INSERT INTO llm_configs
                (id, user_id, name, base_url, api_key, model, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (config_id, user_id, name, base_url, api_key, model, created_at),
        )
        db.commit()
    return {
        "id": config_id,
        "user_id": user_id,
        "name": name,
        "base_url": base_url,
        "model": model,
        "created_at": created_at,
    }


async def list_llm_configs(user_id: str) -> list[dict[str, Any]]:
    with _connect() as db:
        cursor = db.execute(
            """
            SELECT id, user_id, name, base_url, model, created_at
            FROM llm_configs
            WHERE user_id = ?
            ORDER BY created_at DESC
            """,
            (user_id,),
        )
        return [dict(row) for row in cursor.fetchall()]


async def get_llm_config(user_id: str, config_id: str) -> dict[str, Any] | None:
    with _connect() as db:
        cursor = db.execute(
            """
            SELECT id, user_id, name, base_url, api_key, model, created_at
            FROM llm_configs
            WHERE user_id = ? AND id = ?
            """,
            (user_id, config_id),
        )
        return _row_to_dict(cursor.fetchone())


async def create_chat(user_id: str, llm_config_id: str, title: str) -> dict[str, Any]:
    chat_id = str(uuid.uuid4())
    created_at = _now()
    with _connect() as db:
        db.execute(
            """
            INSERT INTO chats
                (id, user_id, llm_config_id, title, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (chat_id, user_id, llm_config_id, title, created_at, created_at),
        )
        db.commit()
    return {
        "id": chat_id,
        "user_id": user_id,
        "llm_config_id": llm_config_id,
        "title": title,
        "created_at": created_at,
        "updated_at": created_at,
    }


async def list_chats(user_id: str) -> list[dict[str, Any]]:
    with _connect() as db:
        cursor = db.execute(
            """
            SELECT id, user_id, llm_config_id, title, created_at, updated_at
            FROM chats
            WHERE user_id = ?
            ORDER BY updated_at DESC
            """,
            (user_id,),
        )
        return [dict(row) for row in cursor.fetchall()]


async def get_chat(user_id: str, chat_id: str) -> dict[str, Any] | None:
    with _connect() as db:
        cursor = db.execute(
            """
            SELECT id, user_id, llm_config_id, title, created_at, updated_at
            FROM chats
            WHERE user_id = ? AND id = ?
            """,
            (user_id, chat_id),
        )
        return _row_to_dict(cursor.fetchone())


async def add_message(
    user_id: str,
    chat_id: str,
    role: str,
    content: str,
) -> dict[str, Any]:
    message_id = str(uuid.uuid4())
    created_at = _now()
    with _connect() as db:
        db.execute(
            """
            INSERT INTO messages (id, chat_id, user_id, role, content, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (message_id, chat_id, user_id, role, content, created_at),
        )
        db.execute(
            "UPDATE chats SET updated_at = ? WHERE id = ? AND user_id = ?",
            (created_at, chat_id, user_id),
        )
        db.commit()
    return {
        "id": message_id,
        "chat_id": chat_id,
        "role": role,
        "content": content,
        "created_at": created_at,
    }


async def list_messages(user_id: str, chat_id: str) -> list[dict[str, Any]]:
    with _connect() as db:
        cursor = db.execute(
            """
            SELECT id, chat_id, role, content, created_at
            FROM messages
            WHERE user_id = ? AND chat_id = ?
            ORDER BY created_at ASC
            """,
            (user_id, chat_id),
        )
        return [dict(row) for row in cursor.fetchall()]


async def create_mcp_config(
    user_id: str,
    name: str,
    url: str,
    token: str,
) -> dict[str, Any]:
    config_id = str(uuid.uuid4())
    created_at = _now()
    with _connect() as db:
        db.execute(
            """
            INSERT INTO mcp_configs (id, user_id, name, url, token, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (config_id, user_id, name, url, token, created_at),
        )
        db.commit()
    return {
        "id": config_id,
        "user_id": user_id,
        "name": name,
        "url": url,
        "created_at": created_at,
    }


async def list_mcp_configs(user_id: str) -> list[dict[str, Any]]:
    with _connect() as db:
        cursor = db.execute(
            """
            SELECT id, user_id, name, url, created_at
            FROM mcp_configs
            WHERE user_id = ?
            ORDER BY created_at DESC
            """,
            (user_id,),
        )
        return [dict(row) for row in cursor.fetchall()]


async def get_mcp_config(user_id: str, config_id: str) -> dict[str, Any] | None:
    with _connect() as db:
        cursor = db.execute(
            """
            SELECT id, user_id, name, url, token, created_at
            FROM mcp_configs
            WHERE user_id = ? AND id = ?
            """,
            (user_id, config_id),
        )
        return _row_to_dict(cursor.fetchone())


async def connect_mcp_to_chat(
    user_id: str,
    chat_id: str,
    mcp_config_id: str,
) -> dict[str, Any]:
    created_at = _now()
    with _connect() as db:
        db.execute(
            """
            INSERT OR IGNORE INTO chat_mcp_configs
                (chat_id, user_id, mcp_config_id, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (chat_id, user_id, mcp_config_id, created_at),
        )
        db.commit()
    return {
        "chat_id": chat_id,
        "mcp_config_id": mcp_config_id,
        "created_at": created_at,
    }


async def disconnect_mcp_from_chat(
    user_id: str,
    chat_id: str,
    mcp_config_id: str,
) -> None:
    with _connect() as db:
        db.execute(
            """
            DELETE FROM chat_mcp_configs
            WHERE user_id = ? AND chat_id = ? AND mcp_config_id = ?
            """,
            (user_id, chat_id, mcp_config_id),
        )
        db.commit()


async def list_chat_mcp_ids(user_id: str, chat_id: str) -> list[str]:
    with _connect() as db:
        cursor = db.execute(
            """
            SELECT mcp_config_id
            FROM chat_mcp_configs
            WHERE user_id = ? AND chat_id = ?
            ORDER BY created_at ASC
            """,
            (user_id, chat_id),
        )
        return [row["mcp_config_id"] for row in cursor.fetchall()]
