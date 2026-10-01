import docker
import sys
import psycopg2
import random

def setup():
    shards = ["shard-0", "shard-1", "shard-2"]
    context = docker.from_env()
    for shard in shards:
        try:
            container = context.containers.get(shard)
            if not container.attrs["State"]["Status"] == "running":
                raise docker.errors.NotFound
        except docker.errors.NotFound:
            sys.exit(1)

def seed():
    context = docker.from_env()
    for ctr in context.containers.list():
        port = 5433
        if ctr.name.startswith("shard"):
            conn = psycopg2.connect(
                host = "localhost",
                database = "users_db",
                user = "admin",
                password = "password",
                port = str(port)
            )
            cursor = conn.cursor()
            cursor.execute(f"INSERT INTO users (id, name, email) VALUES ({random.randint(1, 100)}, 'Lekhan{random.randint(1, 100)}', 'lekhan{random.randint(1, 100)}@example.com');")
            conn.commit()
        port += 1
      
def cleanup():
    context = docker.from_env()
    for ctr in context.containers.list():
        port = 5433
        if ctr.name.startswith("shard"):
            conn = psycopg2.connect(
                host = "localhost",
                database = "users_db",
                user = "admin",
                password = "password",
                port = str(port)
            )
            cursor = conn.cursor()
            cursor.execute(f"DELETE FROM users;")
            conn.commit()
        port += 1

if __name__ == "__main__":
    setup()
    # seed()
    cleanup()