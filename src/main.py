import docker

c = docker.from_env()

ctr = c.containers.get("shard-0")

print(ctr)