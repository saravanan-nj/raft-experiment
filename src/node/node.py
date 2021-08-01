import random
import json
import aiofile
import asyncio
import grpc

from voting import vote_pb2
from voting import vote_pb2_grpc
from voting import voting_service

from node.server import Server
from node.http_server import HttpServer


class Node:

    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"

    def __init__(self, config):
        self.config = config
        self.leader = None
        self.nodes = None
        self.current_term = None
        self.last_index = None
        self.listen_addr = None
        self.current_election_term = None
        self.voted_term = None
        self.__no_heartbeat_handler = None
        self.committed_index = None
        self.nodes_info = None
        self.messages = None
        self.grpc_server = Server()
        self.http_server = HttpServer()

    async def setup(self):
        self.state = self.config.get("state", self.FOLLOWER)
        self.leader = self.config.get("leader")
        self.nodes = self.config.get("nodes")
        self.data_file_path = self.config.get("data_file_path")
        self.log_file_path = self.config.get("log_file_path")
        self.nodes_info = {node: None for node in self.nodes}
        (
            self.current_term,
            self.committed_index,
            self.last_index,
            self.messages,
        ) = await self.load_info_from_messages()
        self.listen_addr = self.config.get("listen")
        self.http_listen_addr = self.config.get("http_listen_addr")
        asyncio.create_task(self.__check_for_election())
        self.grpc_server.configure(
            vote_pb2_grpc.add_VotingServiceServicer_to_server,
            voting_service.VotingService,
            self.listen_addr,
            self,
        )
        self.http_server.configure(
            [("/set", self.set), ("/get", self.get)],
            self.http_listen_addr,
        )

    async def load_info_from_messages(self):
        async with aiofile.async_open(self.log_file_path, "r") as f:
            contents = await f.read()
            if contents:
                data = json.loads(contents)
                metadata = data["__metadata__"]
                return (
                    metadata["current_term"],
                    metadata["committed_index"],
                    metadata["committed_index"],
                    data,
                )
            else:
                return 1, None, None, {}

    async def set(self, request):
        data = await request.json()
        if self.state == self.LEADER:
            await self.set_command(data)
        return self.http_server.send_response(
            {"status": "OK", "message": "Command Success"}
        )

    async def get(self, request):
        data = await request.json()
        # print(self.listen_addr, data)
        return self.http_server.send_response(
            {"status": "OK", "message": "Command Success"}
        )

    async def __check_for_election(self):
        if not self.leader:
            self.state = self.CANDIDATE
            await self.__start_election()

    async def __start_election(self):
        print(f"{self.listen_addr} starting election", self.leader)
        random_timeout = random.randint(15, 18)
        random_timeout = 10
        print(f"{self.listen_addr} waiting for timeout {random_timeout}")
        await asyncio.sleep(random_timeout)
        term = self.current_term + 1
        if self.state == self.CANDIDATE:
            votes_count = await self.__request_votes(term, self.committed_index)
            print(f"{self.listen_addr} Received Votes {votes_count}")
            if votes_count > len(self.nodes) // 2:
                self.state = self.LEADER
                self.current_term = term
                await self.__send_heart_beat()
            else:
                asyncio.create_task(self.__check_for_election())
        else:
            return

    async def __request_votes(self, term, index):
        votes = []
        for node in self.nodes:
            if node == self.listen_addr:
                self.current_election_term = term
                vote, _, _ = self._handle_vote_request(term, index)
                votes.append(vote)
                continue
            vote = await self.__request_vote(node, term, index)
            votes.append(vote)
        return len([vote for vote in votes if vote])

    async def __request_vote(self, addr, term, index):
        print(f"{self.listen_addr} requesting vote from {addr}")
        async with grpc.aio.insecure_channel(addr) as channel:
            stub = vote_pb2_grpc.VotingServiceStub(channel)
            try:
                response = await stub.RequestVote(
                    vote_pb2.Proposal(term=term, currentIndex=index, node=self.listen_addr)
                )
            except grpc.aio.AioRpcError as e:
                vote = False
            else:
                vote = response.vote
                self.nodes_info[addr] = min(response.index, index or 0)
            return vote

    async def __send_heart_beat_to_node(self, node):
        try:
            last_index = self.nodes_info[node] + 1
        except (TypeError, KeyError):
            last_index = 0
        try:
            log = json.dumps(self.messages[str(last_index)])
        except KeyError:
            log = ""
        # print(self.listen_addr, log, last_index, node)
        async with grpc.aio.insecure_channel(node) as channel:
            stub = vote_pb2_grpc.VotingServiceStub(channel)
            try:
                response = await stub.HeartBeat(
                    vote_pb2.AppendEntry(
                        currentTerm=self.current_term,
                        currentIndex=last_index,
                        committedIndex=self.committed_index
                        if self.committed_index is not None
                        else -1,
                        node=self.listen_addr,
                        log=log,
                    )
                )
            except grpc.aio.AioRpcError as e:
                # print(self.listen_addr, e)
                pass
            else:
                # print(self.listen_addr, response.received, response.lastIndex)
                if response.received and log:
                    self.nodes_info[node] = last_index
                elif not response.received:
                    self.nodes_info[node] = response.lastIndex

    async def set_command(self, message):
        if not self.state == self.LEADER:
            return
        if self.last_index is not None:
            self.last_index = self.last_index + 1
        elif self.committed_index:
            self.last_index = self.committed_index + 1
        else:
            self.last_index = 0
        self.messages[str(self.last_index)] = message
        asyncio.create_task(self.check_index_committed(self.last_index))

    async def check_index_committed(self, index):
        quorum_size = len(self.nodes) // 2 + 1
        committed_nodes = [
            node
            for node, node_index_ in self.nodes_info.items()
            if node_index_ is not None and node_index_ >= index
        ]
        committed = len(committed_nodes) >= quorum_size
        if committed:
            # print(f"{self.listen_addr} Committed", self.nodes_info.values(), index)
            await self.update_file(self.messages[str(index)])
            self.committed_index = index
            try:
                self.messages["__metadata__"]["committed_index"] = self.committed_index
            except KeyError:
                self.messages["__metadata__"] = {
                    "committed_index": self.committed_index,
                    "current_term": self.current_term,
                }
            await self.update_messages()
        else:
            # print(f"{self.listen_addr} Not Committed")
            event_loop = asyncio.get_event_loop()
            await asyncio.sleep(1)
            asyncio.create_task(self.check_index_committed(index))

    async def __send_heart_beat(self):
        print(f"{self.listen_addr} Sending heart beats to indicate new leader")
        for node in self.nodes:
            if node == self.listen_addr:
                self.nodes_info[node] = self.last_index
                continue
            else:
                asyncio.create_task(self.__send_heart_beat_to_node(node))
        await asyncio.sleep(1)
        await self.__send_heart_beat()
   
    def _handle_vote_request(self, election_term, election_current_index):
        # already voted
        print(f"{self.listen_addr} Voted Term: {self.voted_term}, Election Term: {election_term}, Election Current Index: {election_current_index}")
        if self.voted_term and election_term <= self.voted_term:
            print(f"{self.listen_addr} Not Voting")
            return False, self.committed_index, self.voted_term if self.voted_term else 0
        elif self.committed_index and self.committed_index > election_current_index:
            print(f"{self.listen_addr} Not Voting")
            return False, self.committed_index, self.voted_term if self.voted_term else 0
        self.voted_term = election_term
        print(f"{self.listen_addr} Voting {self.committed_index} Voted Term {self.voted_term}")
        return True, self.committed_index, self.voted_term


    def handle_vote_request(self, request, context):
        election_term = request.term
        election_current_index = request.currentIndex
        return self._handle_vote_request(election_term, election_current_index)
        
    def __no_heartbeat(self):
        self.leader = None
        asyncio.create_task(self.__check_for_election())

    async def update_file(self, update):
        async with aiofile.async_open(self.data_file_path, "r") as data_file:
            contents = await data_file.read()
            if contents:
                data = json.loads(contents)
            else:
                data = {}
            data.update(update)
        async with aiofile.async_open(self.data_file_path, "w") as data_file:
            await data_file.write(json.dumps(data))

    async def update_messages(self):
        async with aiofile.async_open(self.log_file_path, "w") as log_file:
            await log_file.write(json.dumps(self.messages))

    async def handle_heart_beat(self, request, context):
        if self.__no_heartbeat_handler:
            self.__no_heartbeat_handler.cancel()
        if self.current_term > request.currentTerm:
            return False, -100
        if not self.leader and request.currentTerm > self.current_term and request.node:
            self.state = self.FOLLOWER
            self.leader = request.node
            self.current_term = request.currentTerm
            print(f"{self.listen_addr} Following the node {request.node}")
        # log handling
        try:
            expected_index = self.last_index + 1
        except TypeError:
            expected_index = 0
        # print(self.listen_addr, expected_index, self.last_index, self.committed_index, request.currentIndex, request.currentTerm, self.current_term)
        if not request.log and request.currentIndex == expected_index:
            if self.committed_index is None and request.committedIndex is not None:
                try:
                    await self.update_file(self.messages[str(request.committedIndex)])
                except KeyError:
                    return (
                        True,
                        -1 if self.committed_index is None else self.committed_index,
                    )
                self.committed_index = request.committedIndex
                try:
                    self.messages["__metadata__"][
                        "committed_index"
                    ] = self.committed_index
                except KeyError:
                    self.messages["__metadata__"] = {
                        "committed_index": self.committed_index,
                        "current_term": self.current_term,
                    }
                await self.update_messages()
            elif (
                request.committedIndex is not None
                and request.committedIndex < self.committed_index
            ):
                await self.update_file(self.messages[str(request.committedIndex)])
                self.committed_index = request.committedIndex
                try:
                    self.messages["__metadata__"][
                        "committed_index"
                    ] = self.committed_index
                except KeyError:
                    self.messages["__metadata__"] = {
                        "committed_index": self.committed_index,
                        "current_term": self.current_term,
                    }
                await self.update_messages()
            acknowledged = True
        elif request.currentIndex == expected_index:
            print(f"{self.listen_addr} message: {request.log}, index: {request.currentIndex}")
            acknowledged = True
            self.messages[str(expected_index)] = json.loads(request.log)
            self.last_index = expected_index
            if request.committedIndex <= expected_index:
                await self.update_file(json.loads(request.log))
                self.committed_index = expected_index
                try:
                    self.messages["__metadata__"][
                        "committed_index"
                    ] = self.committed_index
                except KeyError:
                    self.messages["__metadata__"] = {
                        "committed_index": self.committed_index,
                        "current_term": self.current_term,
                    }
                await self.update_messages()
        elif request.currentIndex < expected_index and self.current_term and request.currentTerm > self.current_term:
            if request.log:
                self.messages[str(request.currentIndex)] = json.loads(request.log)
                if request.committedIndex <= request.currentIndex:
                    await self.update_file(json.loads(request.log))
                    self.committed_index = request.currentIndex
                    try:
                        self.messages["__metadata__"][
                            "committed_index"
                        ] = self.committed_index
                    except KeyError:
                        self.messages["__metadata__"] = {
                            "committed_index": self.committed_index,
                            "current_term": self.current_term,
                        }
                    await self.update_messages()
            self.last_index = request.currentIndex
            acknowledged = True
        elif request.currentIndex < expected_index:
            acknowledged = False
        else:
            acknowledged = False
        event_loop = asyncio.get_event_loop()
        self.__no_heartbeat_handler = event_loop.call_later(2, self.__no_heartbeat)
        return (
            acknowledged,
            -1 if self.committed_index is None else self.committed_index,
        )

    async def serve(self):
        loop = asyncio.get_running_loop()
        grpc_server_task = loop.create_task(self.grpc_server.serve())
        http_server_task = self.http_server.serve()
        await asyncio.wait([grpc_server_task, http_server_task])
