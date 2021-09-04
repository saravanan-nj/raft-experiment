import asyncio
from node.node import Node


config = {
    "state": "",
    "leader": "",
    "nodes": ["localhost:50051"],
    "current_term": 1,
    "last_index": 0,
    "listen": "localhost:50051",
    "http_listen_addr": "8081",
    "data_file_path": "./data/data_1.json",
    "log_file_path": "./data/log_1.json"
}


async def run():
    node = Node(config)
    await node.setup()
    await node.serve()


if __name__ == "__main__":
    asyncio.run(run())
