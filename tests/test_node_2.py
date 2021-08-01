import asyncio
from node.node import Node


config = {
    "state": "",
    "leader": "",
    "nodes": ["localhost:50051", "localhost:50052", "localhost:50053"],
    "current_term": 1,
    "last_index": 0,
    "listen": "localhost:50052",
    "http_listen_addr": "8082",
    "data_file_path": "./data/data_2.json",
    "log_file_path": "./data/log_2.json"
}


async def run():
    node = Node(config)
    await node.setup()
    await node.serve()


if __name__ == "__main__":
    asyncio.run(run())
