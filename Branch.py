
import grpc
import Branch_pb2_grpc
from Branch_pb2 import MsgRequest, MsgResponse


class Branch(Branch_pb2_grpc.BranchServicer):

    def __init__(self, id, balance, branches):
        # unique ID of the Branch
        self.id = id
        # replica of the Branch's balance
        self.balance = balance
        # the list of process IDs of the branches
        self.branches = branches
        # the list of Client stubs to communicate with the branches
        self.stubList = list()
        # a list of received messages used for writeset
        self.writeset = list()

        # setup channel & client stub for each branch using grpc
    def createStubs(self):
        for branch_id in self.branches:
            if branch_id != self.id:
                port = str(50000 + branch_id)
                channel = grpc.insecure_channel("localhost:" + port)
                self.stubList.append(Branch_pb2_grpc.BranchStub(channel))

    # creates a new event Id and appends it to the writeset
    def updateWriteset(self):

        createEventId = len(self.writeset) + 1
        self.writeset.append(createEventId)

    # verify self.writeset contains all entries from incoming writeset
    def verifyWriteset(self, writeset):
        return all(entry in self.writeset for entry in writeset)

    # receives incoming message request from the customer transaction and starts message processing
    def MsgDelivery(self, request, context):
        if self.verifyWriteset(request.writeset):
            return self.ProcessMsg(request, False)

    # allows for the Branch propagation from incoming msg request
    def MsgPropagation(self, request, context):
        # enforces monotonic writes
        if self.verifyWriteset(request.writeset):
            return self.ProcessMsg(request, True)

    # handle received Msg, generate and return a MsgResponse
    def ProcessMsg(self, request, propagate):

        if request.interface == "query":
            pass
        elif request.interface == "deposit":
            self.balance += request.money

        elif request.interface == "withdraw":
            if self.balance >= request.money:
                self.balance -= request.money

        if request.interface != "query":
            self.updateWriteset()

            if not propagate:
                for stub in self.stubList:
                    stub.MsgPropagation(MsgRequest(
                        interface=request.interface, money=request.money, writeset=request.writeset))

        return MsgResponse(interface=request.interface, money=request.money, writeset=request.writeset)
