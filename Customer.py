import grpc
from Branch_pb2 import MsgRequest
import Branch_pb2_grpc
from time import sleep


class Customer:
    def __init__(self, id, events):
        # unique ID of the Customer
        self.id = id
        # events from the input
        self.events = events
        # a list of received messages used for debugging purpose
        self.recvMsg = list()
        # list for the writeset
        self.writeset = list()

    # TODO: students are expected to send out the events to the Bank
    def executeEvents(self):
        for event in self.events:

            # create the grpc channel and client stub for the branch
            port = str(50000 + event["dest"])
            channel = grpc.insecure_channel(("localhost:" + port))
            stub = Branch_pb2_grpc.BranchStub(channel)

            # event event is query set money to balance
            money = event["money"] if event["interface"] != "query" else 200

            # send response to the branch server
            response = stub.MsgDelivery(MsgRequest(interface=event["interface"], money=money, writeset=self.writeset))

            self.recvMsg.append({"interface": response.interface, "dest": event["dest"], "money": response.money})

            if event["interface"] != "query":
                self.writeset = response.writeset

            # allow time for writeset
            sleep(.25)

        # return output
        return {"id": self.id, "balance": self.recvMsg[-1]["money"]}
