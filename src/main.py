import docker
import sys
import psycopg2
from docker.models.containers import Container
from fastapi import FastAPI
from pydantic import BaseModel
import hashlib


app = FastAPI()

SHARD_PORTS: dict[str, int] = {"shard-0": 5433, "shard-1": 5434, "shard-2": 5435}
USERS_PORT: int = 5436
REPLICAS: int = 100

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
    

@app.post("/register")
def register(user: User):
    try:
        conn: psycopg2.connection = psycopg2.connect(
            host="localhost",
            database="users_db",
            user="admin",
            password="password",
            port="5436",
        )
        
        cursor: psycopg2.cursor = conn.cursor()
        cursor.execute(f"INSERT INTO users (name) VALUES {user.name}")
        conn.commit()
        
    except psycopg2.OperationalError as e:
        print(f"Failed to connect to shard on port 5436: {e}")
        return None
    

def setup():
    shards: list[str] = ["shard-0", "shard-1", "shard-2"]
    context: docker.DockerClient = docker.from_env()
    for shard in shards:
        try:
            container: Container = context.containers.get(shard)
            if not container.attrs["State"]["Status"] == "running":
                raise docker.errors.NotFound
        except docker.errors.NotFound:
            sys.exit(1)


def seed():
    context: docker.DockerClient = docker.from_env()
    connections: list[psycopg2.connection] = []
    
    for ctr in context.containers.list():
        port: int = 5433
        if ctr.name.startswith("shard"):
            try:
                conn: psycopg2.connection = psycopg2.connect(
                    host="localhost",
                    database="tasks_db",
                    user="admin",
                    password="password",
                    port=str(port),
                )
                
                cursor: psycopg2.cursor = conn.cursor()
                cursor.execute(f"CREATE TABLE IF NOT EXISTS tasks (id SERIAL PRIMARY KEY, task VARCHAR(250) NOT NULL);")
                conn.commit()
                connections.append(conn)
                
            except psycopg2.OperationalError as e:
                print(f"Failed to connect to shard on port {port}: {e}")
                return None
        if ctr.name.startswith("user"):
            try:
                conn: psycopg2.connection = psycopg2.connect(
                    host="localhost",
                    database="tasks_db",
                    user="admin",
                    password="password",
                    port=str(port),
                )
                
                cursor: psycopg2.cursor = conn.cursor()
                cursor.execute(f"CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, name VARCHAR(50) UNIQUE NOT NULL);")
                conn.commit()
                        
            except psycopg2.OperationalError as e:
                print(f"Failed to connect to shard on port {port}: {e}")
                return None  
                      
    return connections


def cleanup():
    context: docker.DockerClient = docker.from_env()
    
    for ctr in context.containers.list():
        port: int = 5433
        
        if ctr.name.startswith("shard"):
            conn: psycopg2.connection = psycopg2.connect(
                host="localhost",
                database="users_db",
                user="admin",
                password="password",
                port=str(port),
            )
            
            cursor: psycopg2.cursor = conn.cursor()
            cursor.execute(f"DELETE FROM users;")
            conn.commit()
            
        port += 1


class Node:
    def __init__(self, conn: psycopg2.connection, start: int):
        self.conn = conn
        self.start = start


if __name__ == "__main__":
    # try:
    #     setup()
    #     connections: list[psycopg2.connection] = seed()

    #     nodes: list[Node] = []
        
    #     for i, conn in enumerate(connections):
    #         nodes.append(Node(conn, i * 100 + 1))
        
        
        
    # except Exception as e:
    #     print(f"Error occurred: {e}")
    # finally:
    #     cleanup()
    conn: psycopg2.connection = psycopg2.connect(
                        host="localhost",
                        database="tasks_db",
                        user="admin",
                        password="password",
                        port=5434,
                    )
    shard_conns = {
        "shard-1": conn
    }
    print(build_ring())