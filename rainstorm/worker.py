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
import subprocess
import os
import hashlib
import traceback
import base64

class WorkerServicer(worker_pb2_grpc.WorkerServicer):
    def __init__(self, mapping, src_file, dest_file):

        # Decode and parse
        decoded_mapping = base64.b64decode(mapping).decode()
        self.mapping = json.loads(decoded_mapping)

        print(f"Decoded mapping: {self.mapping}")

        self.exe_file_path = '/home/chaskar2/distributed-logger/rainstorm/exe_files'
        self.src_file = src_file
        self.dest_file = dest_file
        # output_rec format [(id, data_in_tuple_format), ...]
        self.state = {"inp_id_processed": {}, "state": {}, "output_rec": [], "id_counter": 0}
        self.ack_rec = {}
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
            for partition_num, details in task_dict.items():
                hostname, ip = details.split(":")
                if hostname == self.hostname:
                    # Populate the attributes based on the match
                    print(partition_num)
                    self.task_type = task_type
                    
                    if not self.partition_num:
                        self.exe_file_path = os.path.join(self.exe_file_path, self.task_type)
                    else:
                        self.log(f"Updating the mapping, not updating exe file path")
                    
                    self.partition_num = partition_num
                    self.total_partitions = len(task_dict)
                    self.next_stage_tasks = mapping[mapping_keys[i + 1]] if i + 1 < len(mapping_keys) else None
        self.log(f"Extracted details task_type: {self.task_type} exe_file_path: {self.exe_file_path} partition_num: {self.partition_num}, total_partitions: {self.total_partitions}, next_stage_tasks: {self.next_stage_tasks}")
            
    async def create_files_for_state_recovery(self):
        # Create file for storing state_dict
        response = await self.interact_with_hydfs('create', f"{self.task_type}_{self.partition_num}_state")
        self.log(f"HyDFS response for create state recovery file : {response}")
        
        # Get file for storing state_dict
        response = await self.interact_with_hydfs('get', f"{self.task_type}_{self.partition_num}_state")
        self.log(f"HyDFS response for get state recovery file : {response}")

        # Extract the 'content' field
        content = response.get('content', '')
        lines = content.strip().split("\n")
        last_line = lines[-1] if lines else ""
        if last_line:
            self.state = json.loads(last_line)

        # Create file for storing ack rec
        response = await self.interact_with_hydfs('create', f"{self.task_type}_{self.partition_num}_ack")
        self.log(f"HyDFS response for create ack rec storing file : {response}")

        # Get file for storing state_dict
        response = await self.interact_with_hydfs('get', f"{self.task_type}_{self.partition_num}_ack")
        self.log(f"HyDFS response for get ack rec file : {response}")

        # Extract the 'content' field
        content = response.get('content', '')
        lines = content.strip().split("\n")
        last_line = lines[-1] if lines else ""
        if last_line:
            self.ack_rec = json.loads(last_line)

        await self.send_unacked_tuples()


    async def send_unacked_tuples(self):
        """
        Iterates over self.state['output_rec'] and appends items to self.queue if their id is not in self.ack_rec.
        """
        for record in self.state["output_rec"]:
            rec_id, rec_data = record  # Unpack the tuple
            # id in output_rec is in int
            if str(rec_id) not in self.ack_rec:
                await self.queue.put({'id' : rec_id, rec_data[0]: rec_data[1]})  # Append to the queue
                print(f"Added {record} to the queue.")



    async def interact_with_hydfs(self, action, filename, generated_tuple=None):
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
            "client_name": f'rainstorm_{self.task_type}_{self.partition_num}',
            "action": action,
            "filename": filename
        }

        if (action == "create" ):
            message["filesize"] = 0
            message["content"] = ''.encode('utf-8')
        elif (action == "append"):
            message["filesize"] = 0
            message["content"] = json.dumps(generated_tuple).encode('utf-8')

        try:
            client_socket.sendall(msgpack.packb(message) + b"<EOF>")
            print(f"Message for {action} sent successfully to hydfs.")
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
            self.log("Connection from worker to hydfs closed.")
            return msgpack.unpackb(response)
        except Exception as e:
            self.log(f"Error receiving response: {e}")
            client_socket.close()
            self.log("Connection from worker to hydfs closed.")
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
        Monitors the queue, hashes the key in data_to_send to determine the partition,
        selects the remote address, and sends data using the gRPC stub. If sending fails,
        re-adds data to the queue.
        """
        if self.next_stage_tasks is None:
            response = await self.interact_with_hydfs('create', self.dest_file)
            self.log(f"HyDFS response for create dest file : {response}")
            pass
        while True:
            data_to_send = await self.queue.get()  # Wait for an item in the queue
            self.log(f"Dequeued data: {data_to_send} for sending.")

            if self.next_stage_tasks is None:
                response = await self.interact_with_hydfs('append', self.dest_file, data_to_send)
                self.log(f"HyDFS response for append to dest file : {response}")
                if response["status"] == 'append_logged':
                    # Add ack rec to hyDFS
                    self.ack_rec[data_to_send['id']] = 1
                    response = await self.interact_with_hydfs('append', f"{self.task_type}_{self.partition_num}_ack", self.ack_rec)
                    self.log(f"HyDFS response for append to {self.task_type}_{self.partition_num}_ack : {response}")
                else:
                    self.log('Result append to hydfs failed, readding data to queue')
                    await self.queue.put(data_to_send) 
            else:

                # Extract the key from data_to_send (assuming it's a dictionary with 2 element)
                data_itr = iter(data_to_send.keys())
                next(data_itr)
                key = next(data_itr)

                # Hash the key to determine the partition
                hashed_partition = int(hashlib.sha256(key.encode('utf-8')).hexdigest(), 16) % self.total_partitions

                # Get the remote server address based on the hashed partition
                remote_server_address = self.next_stage_tasks[str(hashed_partition)]

                # Create a gRPC channel and stub
                async with grpc.aio.insecure_channel(remote_server_address) as channel:
                    stub = worker_pb2_grpc.WorkerStub(channel)

                    # Attempt to send the data
                    response = await self.send_data_with_retries(
                        stub=stub,
                        data_to_send=data_to_send,
                        remote_server_address=remote_server_address,
                        timeout_seconds=5
                    )

                    if response:
                        self.log(f"Response from server : {response}")
                        if response["status"] == "error":
                            await self.queue.put(data_to_send)
                        else:
                            # Add ack rec to hyDFS
                            self.ack_rec[data_to_send['id']] = 1
                            response = await self.interact_with_hydfs('append', f"{self.task_type}_{self.partition_num}_ack", self.ack_rec)
                            self.log(f"HyDFS response for append to {self.task_type}_{self.partition_num}_ack : {response}")
                            
                    else:
                        self.log(f"Failed to send data to partition {hashed_partition}: {data_to_send}. Re-queuing.")
                        await self.queue.put(data_to_send)  # Re-add to the queue for retry



    async def start_stream(self):
        
        response = await self.interact_with_hydfs('get', self.src_file)
        self.log(f"{response}")
        if response["status"] == "file_not_found":
            return
        file_content = response["content"].splitlines()
        self.log(f"Retrieved file with {len(file_content)} lines.")

        # Compute partition ranges
        total_lines = len(file_content)
        lines_per_partition = total_lines // self.total_partitions
        start_index = int(self.partition_num) * lines_per_partition
        if int(self.partition_num) == self.total_partitions - 1:  # Last partition
            end_index = total_lines + 1
        else:
            end_index = start_index + lines_per_partition

        # Extract lines for this partition
        partition_data = [(i + 1, line) for i, line in enumerate(file_content[start_index:end_index], start=start_index)]
        self.log(f"Streaming partition {self.partition_num} with {len(partition_data)} lines (lines {start_index + 1}-{end_index}).")

        # Add partition data to the queue
        for data in partition_data:
            self.state['id_counter'] += 1
            self.log(f"Queueing data: {data} with id {self.state['id_counter']}")
            # Appending dic of format {(line_num, file_name): line}
            self.state['output_rec'].append((self.state['id_counter'], [f"{data[0]}|{self.src_file}", data[1]]))
            await self.queue.put({'id' : self.state['id_counter'], f"{data[0]}|{self.src_file}" : data[1]})

        # Save state to hyDFS
        response = await self.interact_with_hydfs('append', f"{self.task_type}_{self.partition_num}_state", self.state)
        self.log(f"HyDFS response for append to f'{self.task_type}_{self.partition_num}_state' : {response}")


    async def run_transform_exe(self, data):
        """
        Runs the `transform.exe` file with the specified arguments and returns the output.
        
        Args:
            input_tuple (tuple): The input tuple.

        Returns:
            str: The list of processed tuples
        """
        try:
            input_id = data['id']
            if input_id in self.state['inp_id_processed']:
                return []
            data_itr = iter(data.keys())
            next(data_itr)
            key = next(data_itr)
            input_tuple = (key, data[key])
            # Convert state and input_tuple to strings for command-line arguments
            state_arg = json.dumps(self.state['state'])  # Safely serialize the dictionary to a JSON string
            input_arg = str(input_tuple)  # Convert tuple to string
            
            # Execute the .exe file with the arguments
            result = subprocess.run(
                [self.exe_file_path, "--state", state_arg, "--input", input_arg],
                capture_output=True, text=True, check=True
            )

            # Parse the .exe output (stdout) as JSON
            exe_output = json.loads(result.stdout)

            # Extract updated_state and processed from the .exe output
            updated_state = exe_output["updated_state"]
            generated_list_of_tuples = exe_output["processed"]

            # Update self.state and processed input ids with the new state from the .exe
            self.state['inp_id_processed'][f"{input_id}"] = 1
            self.state['state'] = updated_state

            # Return the list of processed tuples
            return generated_list_of_tuples
        except subprocess.CalledProcessError as e:
            self.log(f"Error running {self.exe_file_path}: {e.stderr}")
            return None



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
            generated_list_of_tuples = await self.run_transform_exe(data)

            # add to queue
            for data in generated_list_of_tuples:
                self.state['id_counter'] += 1
                self.log(f"Queueing data: {data} with id {self.state['id_counter']}")
                # Appending dic of format {(line_num, file_name): line}
                self.state['output_rec'].append((self.state['id_counter'], data))
                await self.queue.put({'id' : self.state['id_counter'], data[0] : data[1]})

            # Save state to hyDFS
            response = await self.interact_with_hydfs('append', f"{self.task_type}_{self.partition_num}_state", self.state)
            self.log(f"HyDFS response for append to f'{self.task_type}_{self.partition_num}_state' : {response}")

            # Create an acknowledgment as a JSON dictionary
            ack = {"status": "received", "details": "Data processed successfully"}

            # Serialize the acknowledgment to a JSON string and return it
            return worker_pb2.AckResponse(ack=json.dumps(ack))
        except Exception as e:
            self.log(f"Error processing request: {e}")
            traceback_str = traceback.format_exc()
            self.log(f"Traceback: {traceback_str}")
            ack = {"status": "error", "details": str(e)}
            return worker_pb2.AckResponse(ack=json.dumps(ack))


    async def UpdateMapping(self, request, context):
        """
        Handles the UpdateMapping gRPC method to update the mapping.
        Args:
            request: The MappingUpdateRequest object containing the new mapping JSON string.
            context: The gRPC context.

        Returns:
            UpdateResponse object with the update status.
        """
        try:
            # Parse the new mapping JSON string
            # decoded_mapping = base64.b64decode(request.mapping).decode()
            # self.mapping = json.loads(decoded_mapping)
            
            # Update self.mapping and reinitialize task-related attributes
            self.mapping = request.mapping
            
            # self.get_task_type(self.mapping)
            
            # Log success
            self.log(f"Mapping updated successfully: {self.mapping}")
            return worker_pb2.UpdateResponse(status="Mapping updated successfully.")
        except json.JSONDecodeError as e:
            error_message = f"Invalid JSON format: {str(e)}"
            self.log(error_message)
            return worker_pb2.UpdateResponse(status=error_message)
        except Exception as e:
            error_message = f"Error updating mapping: {str(e)}"
            self.log(error_message)
            return worker_pb2.UpdateResponse(status=error_message)




async def serve(port, mapping, src_file, dest_file):
    print(f"Recieved Mapping: {base64.b64decode(mapping).decode()}")
    decoded_mapping = base64.b64decode(mapping).decode()
    server = grpc.aio.server(ThreadPoolExecutor(max_workers=10))
    worker_servicer = WorkerServicer(mapping, src_file, dest_file)
    await worker_servicer.create_files_for_state_recovery()
    worker_pb2_grpc.add_WorkerServicer_to_server(worker_servicer, server)
    listen_addr = f"{worker_servicer.hostname}:{port}"
    server.add_insecure_port(listen_addr)
    print(f"gRPC Worker listening on {listen_addr}")
    print(f"Calling start_task")
    await server.start()
    if worker_servicer.task_type == 'source':
        await worker_servicer.start_stream()
    # Start monitoring the queue
    await worker_servicer.monitor_queue()
    await server.wait_for_termination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start the gRPC Worker")
    parser.add_argument("--port", type=int, required=True, help="Port to run the gRPC server on")
    parser.add_argument("--mapping", type=str, required=True, help="Mapping dict")
    parser.add_argument("--src_file", type=str, required=True, help="Source file on hydfs")
    parser.add_argument("--dest_file", type=str, required=True, help="Destination file on hydfs")
    args = parser.parse_args()
    
    asyncio.run(serve(args.port, args.mapping, args.src_file, args.dest_file))
