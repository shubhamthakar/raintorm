import grpc
import rainstorm_pb2
import rainstorm_pb2_grpc

def main():
    import sys
    if len(sys.argv) < 6:
        print("Usage: RainStorm.py <op1_exe> [<op2_exe> <op3_exe>] <hydfs_src_file> <hydfs_dest_filename> <num_tasks>")
        return

    # Parse input arguments
    op_exe_files = sys.argv[1:-3]  # Extract all binary files
    hydfs_src_file = sys.argv[-3]
    hydfs_dest_filename = sys.argv[-2]
    num_tasks = int(sys.argv[-1])

    # Read binary files
    op_exe_names = []
    for op_file in op_exe_files:
        op_exe_names.append(op_file.split("/")[-1])
    
    print(op_exe_names)

    # Read binary files
    op_exes = []
    for op_file in op_exe_files:
        with open(op_file, "rb") as f:
            op_exes.append(f.read())

    # Connect to leader gRPC
    channel = grpc.insecure_channel('fa24-cs425-6901.cs.illinois.edu:50051')
    stub = rainstorm_pb2_grpc.RainStormStub(channel)

    # Submit job
    response = stub.SubmitJob(rainstorm_pb2.JobRequest(
        op_exes=op_exes,
        op_exe_names=op_exe_names,
        hydfs_src_file=hydfs_src_file,
        hydfs_dest_filename=hydfs_dest_filename,
        num_tasks=num_tasks
    ))

    print(f"Response from leader: {response.message}")

if __name__ == "__main__":
    main()
