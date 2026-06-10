from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from typing import Dict
import json
import uuid

app = FastAPI()
templates = Jinja2Templates(directory="templates")

class Room:
    def __init__(self, code, teacher_name, teacher_ws):
        self.code = code
        self.teacher_name = teacher_name
        self.teacher_ws = teacher_ws
        self.students = {}  # {websocket: {"name": str, "hand_raised": bool, "muted": bool, "video_off": bool}}
        self.whiteboard_history = []
        self.chat_history = []
        self.screen_sharing = False
        self.screen_sharer = None
    
    def get_all_users(self):
        users = [{
            "name": self.teacher_name,
            "role": "teacher",
            "hand_raised": False,
            "muted": False,
            "video_off": False
        }]
        for ws, info in self.students.items():
            users.append({
                "name": info["name"],
                "role": "student",
                "hand_raised": info.get("hand_raised", False),
                "muted": info.get("muted", False),
                "video_off": info.get("video_off", False)
            })
        return users
    
    def get_student_ws_by_name(self, name):
        for ws, info in self.students.items():
            if info["name"] == name:
                return ws
        return None
    
    async def broadcast(self, message: dict, exclude=None):
        if self.teacher_ws and self.teacher_ws != exclude:
            try:
                await self.teacher_ws.send_json(message)
            except:
                pass
        
        for ws in list(self.students.keys()):
            if ws != exclude:
                try:
                    await ws.send_json(message)
                except:
                    pass
    
    async def send_to_user(self, username: str, message: dict):
        if self.teacher_name == username and self.teacher_ws:
            try:
                await self.teacher_ws.send_json(message)
                return True
            except:
                pass
        
        ws = self.get_student_ws_by_name(username)
        if ws:
            try:
                await ws.send_json(message)
                return True
            except:
                pass
        return False

rooms_db: Dict[str, Room] = {}

@app.get("/", response_class=HTMLResponse)
async def get(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/create-room")
async def create_room(teacher_name: str = Form(...)):
    room_code = str(uuid.uuid4())[:8]
    return JSONResponse({"room_code": room_code, "teacher_name": teacher_name})

@app.websocket("/ws/{room_code}/{role}/{username}")
async def websocket_endpoint(websocket: WebSocket, room_code: str, role: str, username: str):
    await websocket.accept()
    print(f"🔵 {role} '{username}' وصل شد به اتاق {room_code}")
    
    if room_code not in rooms_db:
        if role == "teacher":
            rooms_db[room_code] = Room(room_code, username, websocket)
            print(f"🆕 اتاق جدید: {room_code}")
        else:
            await websocket.send_json({"type": "error", "message": "❌ اتاق وجود ندارد!"})
            await websocket.close()
            return
    else:
        room = rooms_db[room_code]
        if role == "teacher":
            old_ws = room.teacher_ws
            room.teacher_ws = websocket
            print(f"🔄 معلم reconnect شد: {username}")
            if old_ws:
                try:
                    await old_ws.close()
                except:
                    pass
            
            await room.broadcast({
                "type": "teacher_reconnected",
                "users": room.get_all_users(),
                "screen_sharing": room.screen_sharing
            })
        else:
            to_remove = None
            for ws, info in list(room.students.items()):
                if info["name"] == username:
                    to_remove = ws
                    break
            
            if to_remove:
                try:
                    await to_remove.close()
                except:
                    pass
                del room.students[to_remove]
                print(f"🔄 دانشجوی قبلی حذف شد: {username}")
            
            room.students[websocket] = {
                "name": username,
                "hand_raised": False,
                "muted": True,  # دانشجوها پیش‌فرض mute هستن
                "video_off": False
            }
            print(f"👨‍🎓 دانشجو اضافه شد: {username}")
    
    room = rooms_db[room_code]
    
    # ارسال دیتای اولیه
    await websocket.send_json({
        "type": "init_data",
        "users": room.get_all_users(),
        "whiteboard_history": room.whiteboard_history,
        "chat_history": room.chat_history,
        "screen_sharing": room.screen_sharing,
        "screen_sharer": room.screen_sharer
    })
    
    await room.broadcast({
        "type": "user_joined",
        "username": username,
        "role": role,
        "users": room.get_all_users()
    }, exclude=websocket)
    
    try:
        while True:
            data = await websocket.receive_json()
            msg_type = data.get("type")
            print(f"📨 {username}: {msg_type}")
            
            # ============ چت ============
            if msg_type == "chat":
                chat_msg = {
                    "type": "chat",
                    "sender": username,
                    "role": role,
                    "text": data["text"]
                }
                room.chat_history.append(chat_msg)
                if len(room.chat_history) > 200:
                    room.chat_history = room.chat_history[-200:]
                await room.broadcast(chat_msg)
            
            # ============ بالا بردن دست ============
            elif msg_type == "raise_hand":
                if role == "student":
                    room.students[websocket]["hand_raised"] = True
                    await room.broadcast({
                        "type": "hand_raised",
                        "username": username,
                        "users": room.get_all_users()
                    })
            
            elif msg_type == "lower_hand":
                if role == "student":
                    room.students[websocket]["hand_raised"] = False
                    await room.broadcast({
                        "type": "hand_lowered",
                        "username": username,
                        "users": room.get_all_users()
                    })
            
            # ============ مدیریت میکروفون دانشجو ============
            elif msg_type == "allow_speak":
                if role == "teacher":
                    student_name = data.get("student_name")
                    allow = data.get("allow", True)
                    student_ws = room.get_student_ws_by_name(student_name)
                    
                    if student_ws and student_ws in room.students:
                        room.students[student_ws]["muted"] = not allow
                        room.students[student_ws]["hand_raised"] = False
                        
                        await student_ws.send_json({
                            "type": "speak_permission",
                            "allowed": allow
                        })
                        
                        await room.broadcast({
                            "type": "hand_lowered",
                            "username": student_name,
                            "users": room.get_all_users()
                        })
                        
                        await room.broadcast({
                            "type": "chat",
                            "sender": "سیستم",
                            "role": "system",
                            "text": f"🔊 معلم {'اجازه صحبت' if allow else 'قطع صدا'} به {student_name} {'داد' if allow else 'کرد'}"
                        })
            
            elif msg_type == "mute_all":
                if role == "teacher":
                    for ws in room.students:
                        room.students[ws]["muted"] = True
                        room.students[ws]["hand_raised"] = False
                        try:
                            await ws.send_json({
                                "type": "speak_permission",
                                "allowed": False
                            })
                        except:
                            pass
                    
                    await room.broadcast({
                        "type": "chat",
                        "sender": "سیستم",
                        "role": "system",
                        "text": "🔇 معلم همه دانشجوها را بی‌صدا کرد"
                    })
                    await room.broadcast({
                        "type": "all_muted",
                        "users": room.get_all_users()
                    })
            
            # ============ WebRTC ============
            elif msg_type == "webrtc_offer":
                data["sender"] = username
                target = data.get("target")
                if target == "all" or not target:
                    await room.broadcast(data, exclude=websocket)
                else:
                    await room.send_to_user(target, data)
            
            elif msg_type == "webrtc_answer":
                data["sender"] = username
                await room.send_to_user(data["target"], data)
            
            elif msg_type == "webrtc_ice_candidate":
                data["sender"] = username
                target = data.get("target")
                if target:
                    await room.send_to_user(target, data)
                else:
                    await room.broadcast(data, exclude=websocket)
            
            # ============ اشتراک صفحه ============
            elif msg_type == "screen_share_started":
                if role == "teacher":
                    room.screen_sharing = True
                    room.screen_sharer = username
                    await room.broadcast({
                        "type": "screen_share_started",
                        "sharer": username
                    }, exclude=websocket)
                    print(f"📺 {username} اشتراک صفحه را شروع کرد")
            
            elif msg_type == "screen_share_stopped":
                if role == "teacher":
                    room.screen_sharing = False
                    room.screen_sharer = None
                    await room.broadcast({
                        "type": "screen_share_stopped",
                        "sharer": username
                    }, exclude=websocket)
                    print(f"📺 {username} اشتراک صفحه را متوقف کرد")
            
            elif msg_type == "screen_share_offer":
                data["sender"] = username
                target = data.get("target")
                if target:
                    await room.send_to_user(target, data)
            
            elif msg_type == "screen_share_answer":
                data["sender"] = username
                await room.send_to_user(data["target"], data)
            
            elif msg_type == "screen_share_ice":
                data["sender"] = username
                target = data.get("target")
                if target:
                    await room.send_to_user(target, data)
            
            # ============ تخته سفید ============
            elif msg_type == "whiteboard_draw":
                draw_data = {
                    "type": "whiteboard_draw",
                    "action": data["action"],
                    "x": data.get("x"),
                    "y": data.get("y"),
                    "color": data.get("color"),
                    "size": data.get("size"),
                    "tool": data.get("tool")
                }
                room.whiteboard_history.append(draw_data)
                if len(room.whiteboard_history) > 1000:
                    room.whiteboard_history = room.whiteboard_history[-1000:]
                await room.broadcast(draw_data, exclude=websocket)
            
            elif msg_type == "whiteboard_clear":
                if role == "teacher":
                    room.whiteboard_history = []
                    await room.broadcast({"type": "whiteboard_clear"})
            
            # ============ اخراج دانشجو ============
            elif msg_type == "kick_student":
                if role == "teacher":
                    student_name = data.get("student_name")
                    student_ws = room.get_student_ws_by_name(student_name)
                    if student_ws:
                        try:
                            await student_ws.send_json({
                                "type": "kicked",
                                "message": "شما توسط معلم از کلاس اخراج شدید."
                            })
                            await student_ws.close()
                        except:
                            pass
                        del room.students[student_ws]
                        await room.broadcast({
                            "type": "user_left",
                            "username": student_name,
                            "users": room.get_all_users()
                        })
                        
    except WebSocketDisconnect:
        print(f"🔴 {role} '{username}' قطع شد")
        await handle_disconnect(websocket, room, role, username)
    except Exception as e:
        print(f"❌ خطا: {e}")
        await handle_disconnect(websocket, room, role, username)

async def handle_disconnect(websocket, room, role, username):
    if role == "teacher":
        await room.broadcast({
            "type": "room_closed",
            "message": "معلم کلاس را ترک کرد. اتاق بسته شد."
        })
        for ws in list(room.students.keys()):
            try:
                await ws.close()
            except:
                pass
        if room.code in rooms_db:
            del rooms_db[room.code]
    else:
        if websocket in room.students:
            del room.students[websocket]
        await room.broadcast({
            "type": "user_left",
            "username": username,
            "users": room.get_all_users()
        })