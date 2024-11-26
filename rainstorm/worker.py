import asyncio
import grpc
from concurrent.futures import ThreadPoolExecutor
import worker_pb2
import worker_pb2_grpc
import argparse
import socket
import logging
import json
import msgpack
import math

class WorkerServicer(worker_pb2_grpc.WorkerServicer):
    def __init__(self, mapping, exe_file, src_file, dest_file):
        self.mapping = json.loads(mapping)
        self.exe_file = exe_file
        self.src_file = src_file
        self.dest_file = dest_file

        # Logging
        self.log_file = '/home/chaskar2/distributed-logger/rainstorm/logs/worker.logs'
        self.init_logging()

        self.task_type = None
        self.partition_num = None
        self.total_partitions = None
        self.next_stage_tasks = None
        self.hostname = socket.gethostname()
        self.get_task_type(self.mapping)
        self.queue = asyncio.Queue()
        

    def init_logging(self):
        # Create a specific logger for RingNode
        self.logger = logging.getLogger('RainstormLogger')
        self.logger.setLevel(logging.INFO)

        # Ensure only one handler is added to prevent duplicate logs
        if not self.logger.hasHandlers():
            # Set up file handler
            file_handler = logging.FileHandler(self.log_file, mode='w')
            file_handler.setLevel(logging.INFO)

            # Set up formatter and add it to handler
            formatter = logging.Formatter('%(asctime)s - %(message)s')
            file_handler.setFormatter(formatter)

            # Add handler to logger
            self.logger.addHandler(file_handler)

        self.log("Logging initialized for Rainstorm worker")

    def log(self, message):
        self.logger.info(message)
        print(message)

    def get_task_type(self, mapping):
        mapping_keys = list(mapping.keys())
        for i, task_type in enumerate(mapping_keys):
            task_dict = mapping[task_type]
            print(task_dict)
            for partition_num, details in task_dict.items():
                hostname, ip = details.split(":")
                if hostname == self.hostname:
                    # Populate the attributes based on the match
                    print(partition_num)
                    self.task_type = task_type
                    self.partition_num = partition_num
                    self.total_partitions = len(task_dict)
                    self.next_stage_tasks = mapping[mapping_keys[i + 1]] if i + 1 < len(mapping_keys) else None
        self.log(f"Extracted details task_type: {self.task_type} partition_num: {self.partition_num}, total_partitions: {self.total_partitions}, next_stage_tasks: {self.next_stage_tasks}")
            
    async def get_file_from_hydfs(self):
        # Connect to coord
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((self.hostname, 5001))
            client_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.log("Connected to the HyDFS coordinator.")
        except Exception as e:
            self.log(f"Connection error: {e}")
            client_socket.close()

        # send get req to hyDFS
        message = {
            "client_name": f'rainstorm_source_{self.partition_num}',
            "action": 'get',
            "filename": self.src_file
        }

        try:
            client_socket.sendall(msgpack.packb(message) + b"<EOF>")
            print(f"Message for get sent successfully to hydfs.")
        except Exception as e:
            print(f"Error sending message: {e}")

        # Response from hydfs
        response = b""
        try:
            while True:
                chunk = client_socket.recv(1024)
                if b"<EOF>" in chunk:
                    response += chunk.replace(b"<EOF>", b"")
                    break
                response += chunk
            client_socket.close()
            print("Connection closed.")
            return msgpack.unpackb(response)
        except Exception as e:
            print(f"Error receiving response: {e}")
            client_socket.close()
            print("Connection closed.")
            return None            
        
    
    async def send_data_with_retries(self, stub, data_to_send, remote_server_address, timeout_seconds=10):
        """
        Sends data to the remote server with retries and a timeout.

        Args:
            stub: The gRPC stub for communication.
            data_to_send: The data to be sent (e.g., a tuple).
            remote_server_address: Address of the remote gRPC server.
            timeout_seconds: Timeout for each gRPC call.

        Returns:
            The response from the server if successful, None otherwise.
        """
        try:
            # Serialize data to JSON and send it as part of the gRPC request
            request = worker_pb2.DataRequest(data=json.dumps(data_to_send))
            response = await asyncio.wait_for(
                stub.RecvData(request),
                timeout=timeout_seconds
            )
            # Deserialize the response JSON
            ack = json.loads(response.ack)
            self.log(f"Received acknowledgment: {ack}")
            return ack
        except asyncio.TimeoutError:
            self.log(f"Timeout while waiting for response from {remote_server_address}")
        except grpc.aio.AioRpcError as e:
            self.log(f"gRPC Error: {e}")
        return None  # Return None if call fails

    

    async def monitor_queue(self):
        """
        Monitors the queue and sends data using the gRPC stub. If sending fails, re-adds data to the queue.
        """
        remote_server_address = self.next_stage_tasks[self.partition_num]

        async with grpc.aio.insecure_channel(remote_server_address) as channel:
            stub = worker_pb2_grpc.WorkerStub(channel)

            while True:
                data_to_send = await self.queue.get()  # Wait for an item in the queue
                self.log(f"Dequeued data: {data_to_send} for sending.")
                
                response = await self.send_data_with_retries(
                    stub=stub,
                    data_to_send=data_to_send,
                    remote_server_address=remote_server_address,
                    timeout_seconds=5
                )

                if response:
                    self.log(f"Data sent successfully: {data_to_send}")
                else:
                    self.log(f"Failed to send data: {data_to_send}. Re-queuing.")
                    await self.queue.put(data_to_send)  # Re-add to the queue for retry



    async def start_stream(self):
        remote_server_address = self.next_stage_tasks[self.partition_num]
        response = await self.get_file_from_hydfs()
        self.log(f"{response}")
        self.log(f"Retrieved file with {len(file_content)} lines.")
        if response["status"] == "file_not_found":
            return
        file_content = response["content"].splitlines()

        # Compute partition ranges
        total_lines = len(file_content)
        lines_per_partition = total_lines // self.total_partitions
        start_index = self.partition_num * lines_per_partition
        if self.partition_num == self.total_partitions - 1:  # Last partition
            end_index = total_lines
        else:
            end_index = start_index + lines_per_partition

        # Extract lines for this partition
        partition_data = [(i + 1, line) for i, line in enumerate(file_content[start_index:end_index], start=start_index)]
        self.log(f"Streaming partition {self.partition_num} with {len(partition_data)} lines (lines {start_index + 1}-{end_index}).")

        # Add partition data to the queue
        for data in partition_data:
            self.log(f"Queueing data: {data}")
            # Appending dic of format {(line_num, file_name): line}
            await self.queue.put({(data[0], self.src_file) : data[1]})



    async def RecvData(self, request, context):
        """
        Handles the RecvData gRPC method, logging the received data and sending an acknowledgment back.

        Args:
            request: The DataRequest object containing the serialized JSON data.
            context: The gRPC context.

        Returns:
            AckResponse object containing the acknowledgment as a serialized JSON string.
        """
        try:
            # Deserialize the JSON string from the request
            data = json.loads(request.data)
            self.log(f"Received data: {data}")

            # Create an acknowledgment as a JSON dictionary
            ack = {"status": "received", "details": "Data processed successfully"}

            # Serialize the acknowledgment to a JSON string and return it
            return worker_pb2.AckResponse(ack=json.dumps(ack))
        except Exception as e:
            self.log(f"Error processing request: {e}")
            ack = {"status": "error", "details": str(e)}
            return worker_pb2.AckResponse(ack=json.dumps(ack))



async def serve(port, mapping, exe_file, src_file, dest_file):
    server = grpc.aio.server(ThreadPoolExecutor(max_workers=10))
    worker_servicer = WorkerServicer(mapping, exe_file, src_file, dest_file)
    worker_pb2_grpc.add_WorkerServicer_to_server(worker_servicer, server)
    listen_addr = f"{worker_servicer.hostname}:{port}"
    server.add_insecure_port(listen_addr)
    print(f"gRPC Worker listening on {listen_addr}")
    print(f"Calling start_task")
    await server.start()
    if worker_servicer.task_type == 'source':
        await worker_servicer.start_stream()
    # Start monitoring the queue
    await self.monitor_queue()
    await server.wait_for_termination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start the gRPC Worker")
    parser.add_argument("--port", type=int, required=True, help="Port to run the gRPC server on")
    parser.add_argument("--mapping", type=str, required=True, help="Mapping dict")
    parser.add_argument("--exe_file", type=str, required=True, help="The executable file")
    parser.add_argument("--src_file", type=str, required=True, help="Source file on hydfs")
    parser.add_argument("--dest_file", type=str, required=True, help="Destination file on hydfs")
    args = parser.parse_args()
    
    asyncio.run(serve(args.port, args.mapping, args.exe_file, args.src_file, args.dest_file))
