from http.client import HTTPException

import docker
import sys
import psycopg2
from docker.models.containers import Container
from fastapi import FastAPI
from pydantic import BaseModel
import hashlib
import uvicorn

app = FastAPI()

SHARD_PORTS: dict[str, int] = {"shard-0": 5433, "shard-1": 5434, "shard-2": 5435}
USERS_PORT: int = 5436
REPLICAS: int = 10

users_conn: psycopg2.connection = None
shard_conns: dict[str, psycopg2.connection] = {}    
nodes: list[Node] = []  

class User(BaseModel):
    name: str
    
class Task(BaseModel):
    user: str
    task: str

class Node:
    def __init__(self, shard_name: str, conn: psycopg2.connection, position: int):
        self.shard_name = shard_name
        self.conn = conn
        self.position = position

def hash_key(key: str) -> str:
    digest = hashlib.md5(key.encode()).hexdigest()
    return int(digest, 16)

def build_ring() -> list[Node]:
    ring: list[Node] = []
    
    for shard_name, conn in shard_conns.items():
        for i in range(REPLICAS):
            position = hash_key(f"{shard_name}#{i}")
            ring.append(Node(shard_name, conn, position))
    
    ring.sort(key=lambda node: node.position)
    return ring

def find_node(key: str) -> Node:
    key_hash = hash_key(key)
 
    for node in nodes:
        if node.position >= key_hash:
            return node
 
    return nodes[0] 

def get_user_id(name: str) -> int:
    cursor = users_conn.cursor()
    cursor.execute("SELECT id FROM users WHERE name = %s", (name,))
    row = cursor.fetchone()
    users_conn.commit()
 
    if row is None:
        raise HTTPException(status_code=404, detail=f"User '{name}' not found")
    
    return row[0]   

@app.post("/register")
def register(user: User):
    try:
        cursor = users_conn.cursor()
        cursor.execute(
            "INSERT INTO users (name) VALUES (%s) RETURNING id", (user.name,)
        )
        user_id = cursor.fetchone()[0]
        users_conn.commit()
    except psycopg2.errors.UniqueViolation:
        users_conn.rollback()
        raise HTTPException(status_code=409, detail="User already exists")
 
    node = find_node(str(user_id))
    return {"id": user_id, "name": user.name, "shard": node.shard_name}

@app.post("/tasks")
def create_task(body: Task):
    user_id = get_user_id(body.user)
    node = find_node(str(user_id)) 
 
    cursor = node.conn.cursor()
    cursor.execute(
        "INSERT INTO tasks (user_id, task) VALUES (%s, %s) RETURNING id",
        (user_id, body.task),
    )
    task_id = cursor.fetchone()[0]
    node.conn.commit()
 
    return {"task_id": task_id, "user": body.user, "shard": node.shard_name}    

@app.get("/tasks/{user}")
def list_tasks(user: str):
    user_id = get_user_id(user)
    node = find_node(str(user_id))
 
    cursor = node.conn.cursor()
    cursor.execute("SELECT id, task FROM tasks WHERE user_id = %s", (user_id,))
    rows = cursor.fetchall()
    node.conn.commit()
 
    return {"user": user, "shard": node.shard_name, "tasks": rows}

@app.get("/stats")
def stats():
    result = {}
    for shard_name, conn in shard_conns.items():
        cursor = conn.cursor()
        cursor.execute("SELECT user_id, COUNT(*) FROM tasks GROUP BY user_id")
        result[shard_name] = {str(uid): count for uid, count in cursor.fetchall()}
        conn.commit()
    return result

def setup():
    names = ["user_db"] + list(SHARD_PORTS.keys())
    context = docker.from_env()
    for name in names:
        try:
            container = context.containers.get(name)
            if container.attrs["State"]["Status"] != "running":
                raise docker.errors.NotFound(f"{name} is not running")
        except docker.errors.NotFound:
            print(f"Container {name} is not running. Run: docker compose up -d")
            sys.exit(1)


def seed():
    users = psycopg2.connect(
        host="localhost",
        database="users_db",
        user="admin",
        password="password",
        port=USERS_PORT,
    )
    cursor = users.cursor()
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS users "
        "(id SERIAL PRIMARY KEY, name VARCHAR(50) UNIQUE NOT NULL);"
    )
    users.commit()

    shards = {}
    for shard_name, port in SHARD_PORTS.items():
        conn = psycopg2.connect(
            host="localhost",
            database="tasks_db",
            user="admin",
            password="password",
            port=port,
        )
        cursor = conn.cursor()
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS tasks "
            "(id SERIAL PRIMARY KEY, user_id INT NOT NULL, task VARCHAR(250) NOT NULL);"
        )
        conn.commit()
        shards[shard_name] = conn
 
    return users, shards


def cleanup():
    if users_conn is not None:
        cursor = users_conn.cursor()
        cursor.execute("TRUNCATE users RESTART IDENTITY;")
        users_conn.commit()
        users_conn.close()
 
    for conn in shard_conns.values():
        cursor = conn.cursor()
        cursor.execute("TRUNCATE tasks RESTART IDENTITY;")
        conn.commit()
        conn.close()


if __name__ == "__main__":
    try:
        setup()
        users_conn, shard_conns = seed()
        nodes = build_ring()
 
        uvicorn.run(app, host="127.0.0.1", port=8000)
 
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        cleanup()