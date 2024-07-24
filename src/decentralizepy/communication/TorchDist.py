from collections import deque
import datetime
import logging
import pickle
import numpy
import torch
import torch.distributed as dist


class TorchDist:
    """
    Communcation API

    """

    def __init__(
        self,
        rank,
        machine_id,
        mapping,
        total_procs,
        addresses_filepath,
        offset=9000,
        recv_timeout=50,
    ):
        """
        Constructor

        Parameters
        ----------
        rank : int
            Local rank of the process
        machine_id : int
            Machine id of the process
        mapping : decentralizepy.mappings.Mapping
            uid, rank, machine_id invertible mapping
        total_procs : int
            Total number of processes

        """
        self.total_procs = total_procs
        self.rank = rank
        self.machine_id = machine_id
        self.mapping = mapping
        self.uid = mapping.get_uid(rank, machine_id)
        self.total_bytes = 0
        dist.init_process_group("gloo", rank=rank, world_size=total_procs)
        self.total_data = 0
        self.total_meta = 0
        self.received_this_round = 0

    def encrypt(self, data):
        """
        Encode data as python pickle.

        Parameters
        ----------
        data : dict
            Data dict to send

        Returns
        -------
        byte
            Encoded data

        """
        data_len = 0
        if "params" in data:
            data_len = len(pickle.dumps(data["params"]))
        output = pickle.dumps(data)
        self.total_meta += len(output) - data_len
        self.total_data += data_len
        return output

    def decrypt(self, sender, data):
        """
        Decode received pickle data.

        Parameters
        ----------
        sender : byte
            sender of the data
        data : byte
            Data received

        Returns
        -------
        tuple
            (sender: int, data: dict)

        """

        data = pickle.loads(data)
        return data

    def connect_neighbors(self, neighbors):
        """
        Connects all neighbors.

        Parameters
        ----------
        neighbors : list(int)
            List of neighbors

        """
        pass

    def receive(self, channel, block):
        """
        Returns ONE message received.

        Returns
        ----------
        dict
            Received and decrypted data

        """
        byte_tensor = torch.tensor(0)
        try:

            request = dist.irecv(byte_tensor, tag=channel)
            request.wait(datetime.timedelta(milliseconds=50))
            if request.is_completed():
                logging.debug(
                    "Received some message from {} with CHANNEL: {}".format(
                        request.source_rank(), channel
                    )
                )
                return (
                    request.source_rank(),
                    self.decrypt(request.source_rank, bytes(byte_tensor.tolist())),
                )
            else:
                return None
        except:
            return None

    def send(self, uid, data, channel):
        """
        Send a message to a process.

        Parameters
        ----------
        uid : int
            Neighbor's unique ID
        data : dict
            Message as a Python dictionary

        """
        data = self.encrypt(data)
        data = torch.frombuffer(numpy.array(data), dtype=torch.uint8)
        data_size = len(data)
        logging.debug("{} sent the message to {}.".format(self.uid, uid))
        logging.debug("Sent message size: {}".format(data_size))
        dist.isend(data, uid, tag=channel)

    def disconnect_neighbors(self):
        """
        Disconnects all neighbors.

        """
        pass

    def terminate(self):
        """
        Terminate the communication sockets.

        """
        pass

    def already_connected(self, neighbor):
        return True

    def destroy_connection(self, neighbor, linger=None):
        pass

    def init_connection(self, neighbor):
        pass
