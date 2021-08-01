import grpc
from voting import vote_pb2
from voting import vote_pb2_grpc


class VotingService(vote_pb2_grpc.VotingServiceServicer):
    def __init__(self, node):
        self.node = node
        super().__init__()

    async def RequestVote(self, request, context):
        vote, index, term = self.node.handle_vote_request(request, context)
        return vote_pb2.ReceivedVote(vote=vote, node=self.node.listen_addr, index=index, term=term)

    async def HeartBeat(self, request, context):
        acknowledged, last_index = await self.node.handle_heart_beat(request, context)
        return vote_pb2.Acknowledgement(
            received=acknowledged, node=self.node.listen_addr, lastIndex=last_index
        )


async def request_vote(term, currentIndex):
    async with grpc.aio.insecure_channel("localhost:50051") as channel:
        stub = vote_pb2_grpc.VotingServiceStub(channel)
        response = await stub.RequestVote(
            vote_pb2.Proposal(
                term=term, currentIndex=currentIndex, node=self.node.listen_addr
            )
        )
