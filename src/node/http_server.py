import asyncio
from aiohttp import web


class HttpServer:
    def configure(self, routes_config, port):
        self.routes_config = routes_config
        self.port = port
        self.app = web.Application()
        self.add_routes()

    def add_routes(self):
        routes = [web.post(path, handler) for path, handler in self.routes_config]
        self.app.add_routes(routes)

    def send_response(self, data):
        return web.json_response(data)

    def serve(self):
        loop = asyncio.get_running_loop()
        return loop.create_task(web._run_app(self.app, host="0.0.0.0", port=self.port))
