import asyncio
import grpc
import os

import rainstorm_pb2
import rainstorm_pb2_grpc

class Leader(rainstorm_pb2_grpc.RainStormServicer):
    def __init__(self, base_dir="temp_files"):
        self.base_dir = base_dir
        os.makedirs(base_dir, exist_ok=True)

    async def SubmitJob(self, request, context):
        # Save binaries dynamically
        for i, binary in enumerate(request.op_exes):
            op_path = os.path.join(self.base_dir, f"op{i+1}_exe")
            with open(op_path, "wb") as f:
                f.write(binary)
            print(f"Saved binary {i+1} at {op_path}")

        print(f"Received job with {request.num_tasks} tasks")
        print(f"Source file: {request.hydfs_src_file}")
        print(f"Destination file: {request.hydfs_dest_filename}")

        # Add async logic for job execution here
        await asyncio.sleep(1)  # Simulate async task handling
        
        return rainstorm_pb2.JobResponse(message="Job submitted successfully!")

async def serve():
    server = grpc.aio.server()
    rainstorm_pb2_grpc.add_RainStormServicer_to_server(Leader(), server)
    server.add_insecure_port('fa24-cs425-6901.cs.illinois.edu:50051')
    print("Leader is running on port 50051...")
    await server.start()
    await server.wait_for_termination()

if __name__ == "__main__":
    asyncio.run(serve())
