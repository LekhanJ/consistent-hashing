import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5434,
    database="tasks_db",
    user="admin",
    password="password",
)

print(conn)

print("Connected!")