import asyncio
import grpc
import vote_pb2_grpc
import vote_pb2
from voting_service import request_vote


if __name__ == "__main__":
    asyncio.run(request_vote(1, 0))

