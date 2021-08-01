import asyncio
import grpc
import vote_pb2_grpc

from voting_service import VotingService


async def serve() -> None:
    server = grpc.aio.server()
    vote_pb2_grpc.add_VotingServiceServicer_to_server(VotingService(), server)
    listen_addr = '[::]:50051'
    server.add_insecure_port(listen_addr)
    print("starting SErver")
    await server.start()
    try:
        await server.wait_for_termination()
    except KeyboardInterrupt:
        # Shuts down the server with 0 seconds of grace period. During the
        # grace period, the server won't accept new connections and allow
        # existing RPCs to continue within the grace period.
        await server.stop(0)


if __name__ == '__main__':
    asyncio.run(serve())
