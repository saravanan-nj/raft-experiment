import grpc


class Server:

    def __init__(self, servicer_to_server=None, service=None, listen_addr=None):
        self.servicer_to_server = servicer_to_server
        self.service = service
        self.listen_addr = listen_addr
        self.server = None
        self.node = None
    
    def configure(self, servicer_to_server, service, listen_addr, node):
        self.servicer_to_server = servicer_to_server
        self.service = service
        self.listen_addr = listen_addr
        self.node = node
     
    async def restart(self): 
        await self.stop()
        await self.serve()
    
    async def stop(self):
        await self.server.stop(0)

    async def serve(self):
        self.server = grpc.aio.server()
        self.servicer_to_server(self.service(self.node), self.server)
        self.server.add_insecure_port(self.listen_addr)
        print(f"Starting Server at {self.listen_addr}")
        await self.server.start()
        try:
            await self.server.wait_for_termination()
        except KeyboardInterrupt:    
            await self.server.stop(0)

