from contextlib import asynccontextmanager
from fastapi import Depends, FastAPI, HTTPException, status
from pydantic import BaseModel, Field

from .auth import (
    TOKEN_EXPIRE_SECONDS,
    create_token,
    get_current_user,
    hash_password,
    verify_password,
)
from .db import (
    add_message,
    connect_mcp_to_chat,
    create_chat,
    create_llm_config,
    create_mcp_config,
    create_user,
    disconnect_mcp_from_chat,
    get_chat,
    get_llm_config,
    get_mcp_config,
    get_user_by_id,
    get_user_by_login,
    init_db,
    list_chat_mcp_ids,
    list_chats,
    list_llm_configs,
    list_mcp_configs,
    list_messages,
)
from .llm import llm_error_message, run_agent_iteration
from .mcp_tools import BUILTIN_MCP_ID, builtin_mcp_config, build_openai_tools


class RegisterRequest(BaseModel):
    login: str = Field(min_length=1, max_length=128)
    password: str = Field(min_length=8, max_length=256)


class LoginRequest(BaseModel):
    login: str = Field(min_length=1, max_length=128)
    password: str = Field(min_length=1, max_length=256)


class TokenResponse(BaseModel):
    user_id: str
    access_token: str
    token_type: str = "bearer"
    expires_in: int = TOKEN_EXPIRE_SECONDS


class MeResponse(BaseModel):
    user_id: str
    login: str


class LLMConfigCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=128)
    base_url: str = Field(min_length=1, max_length=512)
    api_key: str = Field(min_length=1, max_length=4096)
    model: str = Field(min_length=1, max_length=256)


class LLMConfigResponse(BaseModel):
    id: str
    user_id: str
    name: str
    base_url: str
    model: str
    created_at: str


class ChatCreateRequest(BaseModel):
    llm_config_id: str
    title: str = Field(default="New chat", min_length=1, max_length=256)


class ChatResponse(BaseModel):
    id: str
    user_id: str
    llm_config_id: str
    title: str
    created_at: str
    updated_at: str


class MessageCreateRequest(BaseModel):
    content: str = Field(min_length=1, max_length=20000)


class MessageResponse(BaseModel):
    id: str
    chat_id: str
    role: str
    content: str
    created_at: str


class ChatCompletionResponse(BaseModel):
    user_message: MessageResponse
    assistant_message: MessageResponse


class MCPConfigCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=32, pattern=r"^[A-Za-z0-9_-]+$")
    url: str = Field(min_length=1, max_length=512)
    token: str = Field(min_length=1, max_length=4096)


class MCPConfigResponse(BaseModel):
    id: str
    user_id: str
    name: str
    url: str
    created_at: str


class ChatMCPConnectionResponse(BaseModel):
    chat_id: str
    mcp_config_id: str
    created_at: str | None = None


# --- startup ---
async def startup():
    await init_db()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await startup()
    yield


app = FastAPI(
    title="Simple Agent API",
    description="it's work",
    lifespan=lifespan,
)
main = app


@app.post(
    "/auth/register", response_model=TokenResponse, status_code=status.HTTP_201_CREATED
)
async def register(data: RegisterRequest):
    login = data.login.strip()
    if not login:
        raise HTTPException(status_code=422, detail="login must not be blank")

    existing = await get_user_by_login(login)
    if existing:
        raise HTTPException(status_code=409, detail="User already exists")

    user_id = await create_user(login, hash_password(data.password))

    return {
        "user_id": user_id,
        "access_token": create_token(user_id),
    }


@app.post("/auth/login", response_model=TokenResponse)
async def login(data: LoginRequest):
    user = await get_user_by_login(data.login.strip())

    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    user_id, _login, password_hash = user

    if not verify_password(data.password, password_hash):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = create_token(user_id)

    return {
        "user_id": user_id,
        "access_token": token,
    }


@app.get("/me", response_model=MeResponse)
async def me(user_id: str = Depends(get_current_user)):
    user = await get_user_by_id(user_id)
    if user is None:
        raise HTTPException(status_code=401, detail="Invalid token")

    user_id, login = user
    return {"user_id": user_id, "login": login}


@app.post(
    "/llm-configs",
    response_model=LLMConfigResponse,
    status_code=status.HTTP_201_CREATED,
)
async def add_llm_config(
    data: LLMConfigCreateRequest,
    user_id: str = Depends(get_current_user),
):
    name = data.name.strip()
    base_url = data.base_url.strip().rstrip("/")
    model = data.model.strip()
    if not name or not base_url or not model:
        raise HTTPException(
            status_code=422,
            detail="name, base_url and model are required",
        )

    return await create_llm_config(
        user_id=user_id,
        name=name,
        base_url=base_url,
        api_key=data.api_key,
        model=model,
    )


@app.get("/llm-configs", response_model=list[LLMConfigResponse])
async def get_llm_configs(user_id: str = Depends(get_current_user)):
    return await list_llm_configs(user_id)


@app.post(
    "/mcp-configs",
    response_model=MCPConfigResponse,
    status_code=status.HTTP_201_CREATED,
)
async def add_mcp_config(
    data: MCPConfigCreateRequest,
    user_id: str = Depends(get_current_user),
):
    return await create_mcp_config(
        user_id=user_id,
        name=data.name.strip(),
        url=data.url.strip(),
        token=data.token,
    )


@app.get("/mcp-configs", response_model=list[MCPConfigResponse])
async def get_mcp_configs(user_id: str = Depends(get_current_user)):
    user_configs = await list_mcp_configs(user_id)
    return [builtin_mcp_config(), *user_configs]


@app.post("/chats", response_model=ChatResponse, status_code=status.HTTP_201_CREATED)
async def add_chat(
    data: ChatCreateRequest,
    user_id: str = Depends(get_current_user),
):
    llm_config = await get_llm_config(user_id, data.llm_config_id)
    if llm_config is None:
        raise HTTPException(status_code=404, detail="LLM config not found")

    title = data.title.strip()
    if not title:
        raise HTTPException(status_code=422, detail="title must not be blank")

    return await create_chat(user_id, data.llm_config_id, title)


@app.get("/chats", response_model=list[ChatResponse])
async def get_chats(user_id: str = Depends(get_current_user)):
    return await list_chats(user_id)


async def _get_chat_mcp_configs(user_id: str, chat_id: str) -> list[dict]:
    config_ids = await list_chat_mcp_ids(user_id, chat_id)
    configs = []
    for config_id in config_ids:
        if config_id == BUILTIN_MCP_ID:
            configs.append(builtin_mcp_config())
            continue
        config = await get_mcp_config(user_id, config_id)
        if config is not None:
            configs.append(config)
    return configs


@app.get("/chats/{chat_id}/mcp-configs", response_model=list[MCPConfigResponse])
async def get_chat_mcp_configs(
    chat_id: str,
    user_id: str = Depends(get_current_user),
):
    chat = await get_chat(user_id, chat_id)
    if chat is None:
        raise HTTPException(status_code=404, detail="Chat not found")
    return await _get_chat_mcp_configs(user_id, chat_id)


@app.post(
    "/chats/{chat_id}/mcp-configs/{mcp_config_id}",
    response_model=ChatMCPConnectionResponse,
    status_code=status.HTTP_201_CREATED,
)
async def connect_chat_mcp_config(
    chat_id: str,
    mcp_config_id: str,
    user_id: str = Depends(get_current_user),
):
    chat = await get_chat(user_id, chat_id)
    if chat is None:
        raise HTTPException(status_code=404, detail="Chat not found")
    if (
        mcp_config_id != BUILTIN_MCP_ID
        and await get_mcp_config(user_id, mcp_config_id) is None
    ):
        raise HTTPException(status_code=404, detail="MCP config not found")
    return await connect_mcp_to_chat(user_id, chat_id, mcp_config_id)


@app.delete(
    "/chats/{chat_id}/mcp-configs/{mcp_config_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def disconnect_chat_mcp_config(
    chat_id: str,
    mcp_config_id: str,
    user_id: str = Depends(get_current_user),
):
    chat = await get_chat(user_id, chat_id)
    if chat is None:
        raise HTTPException(status_code=404, detail="Chat not found")
    await disconnect_mcp_from_chat(user_id, chat_id, mcp_config_id)


@app.get("/chats/{chat_id}/messages", response_model=list[MessageResponse])
async def get_chat_messages(
    chat_id: str,
    user_id: str = Depends(get_current_user),
):
    chat = await get_chat(user_id, chat_id)
    if chat is None:
        raise HTTPException(status_code=404, detail="Chat not found")

    return await list_messages(user_id, chat_id)


@app.post("/chats/{chat_id}/messages", response_model=ChatCompletionResponse)
async def send_chat_message(
    chat_id: str,
    data: MessageCreateRequest,
    user_id: str = Depends(get_current_user),
):
    chat = await get_chat(user_id, chat_id)
    if chat is None:
        raise HTTPException(status_code=404, detail="Chat not found")

    llm_config = await get_llm_config(user_id, chat["llm_config_id"])
    if llm_config is None:
        raise HTTPException(status_code=404, detail="LLM config not found")

    content = data.content.strip()
    if not content:
        raise HTTPException(status_code=422, detail="content must not be blank")

    user_message = await add_message(user_id, chat_id, "user", content)
    history = await list_messages(user_id, chat_id)
    mcp_configs = await _get_chat_mcp_configs(user_id, chat_id)

    try:
        tools = await build_openai_tools(mcp_configs)

        assistant_content = await run_agent_iteration(
            llm_config,
            history,
            tools,
            mcp_configs,
        )
    except Exception as error:
        raise HTTPException(
            status_code=502,
            detail=llm_error_message(error),
        ) from error

    assistant_message = await add_message(
        user_id,
        chat_id,
        "assistant",
        assistant_content,
    )
    return {
        "user_message": user_message,
        "assistant_message": assistant_message,
    }

