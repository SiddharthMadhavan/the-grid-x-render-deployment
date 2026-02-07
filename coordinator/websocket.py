# coordinator/websocket.py
from fastapi import WebSocket, WebSocketDisconnect
import json, uuid
from typing import Optional

from .workers import (
    register_worker_ws, unregister_worker_ws,
    update_worker_last_seen, lock
)
from .database import (
    db_upsert_worker, db_set_worker_offline, get_db
)
from .scheduler import dispatch, on_job_started, on_job_result


async def worker_ws(websocket: WebSocket):
    await websocket.accept()
    worker_id: Optional[str] = None

    try:
        while True:
            msg = await websocket.receive_json()
            t = msg.get("type")

            if t == "hello":
                worker_id = msg.get("worker_id") or str(uuid.uuid4())
                caps = msg.get("caps", {})
                owner_id = msg.get("owner_id", "")

                async with lock:
                    register_worker_ws(worker_id, websocket, caps, owner_id)

                db_upsert_worker(worker_id, "remote", caps, "idle", owner_id)
                await websocket.send_json({"type": "hello_ack", "worker_id": worker_id})
                await dispatch()
                continue

            if not worker_id:
                continue

            update_worker_last_seen(worker_id)

            if t == "job_started":
                on_job_started(msg.get("job_id"))

            if t == "job_result":
                on_job_result(
                    msg.get("job_id"),
                    worker_id,
                    msg.get("exit_code", 0),
                    msg.get("stdout", ""),
                    msg.get("stderr", ""),
                    msg.get("duration_seconds"),
                )
                await dispatch()

    except WebSocketDisconnect:
        pass
    finally:
        if worker_id:
            async with lock:
                unregister_worker_ws(worker_id)
            db_set_worker_offline(worker_id)
