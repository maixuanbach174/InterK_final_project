import json
from json import JSONDecodeError
from typing import NoReturn

from fastapi import APIRouter, WebSocket

from dbcsv.app.security.auth import auth_manager

router = APIRouter(tags=["Websocket"])


@router.websocket("/ws")
async def websocket_endpoints(websocket: WebSocket) -> NoReturn:
    await websocket.accept()

    user_authenticated = False

    while True:
        data = await websocket.receive_text()
        try:
            json_data = json.loads(data)
        except JSONDecodeError:
            await websocket.send_json({"message": "The data must be in json"})
            continue

        action = json_data["action"]

        if action == "connect":
            auth_manager.authenticate_user(json_data["username"], json_data["password"])
            user_authenticated = True
            await websocket.send_json({"message": "authenticated"})

        elif action == "query" and user_authenticated:
            await websocket.send_json({"message": "Chua hoan thanh hihihihih"})
