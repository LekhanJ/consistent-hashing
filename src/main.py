import docker
import sys
import psycopg2
import random
from docker.models.containers import Container


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
    offset: int = 0
    connections: list[psycopg2.connection] = []
    
    for ctr in context.containers.list():
        port: int = 5433
        if ctr.name.startswith("shard"):
            try:
                conn: psycopg2.connection = psycopg2.connect(
                    host="localhost",
                    database="users_db",
                    user="admin",
                    password="password",
                    port=str(port),
                )
                
                cursor: psycopg2.cursor = conn.cursor()
                start: int = offset * 100 + 1
                
                for i in range(5):
                    cursor.execute(
                        f"INSERT INTO users (id, name, email) VALUES ({start + i}, 'Lekhan{random.randint(1, 100)}', 'lekhan{random.randint(1, 100)}@example.com');"
                    )
                    
                conn.commit()
                connections.append(conn)
                
            except psycopg2.OperationalError as e:
                print(f"Failed to connect to shard on port {port}: {e}")
                return None
            
        offset += 1
        port += 1
        
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
    try:
        setup()
        connections: list[psycopg2.connection] = seed()

        nodes: list[Node] = []
        
        for i, conn in enumerate(connections):
            nodes.append(Node(conn, i * 100 + 1))
        
        
        
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        cleanup()
