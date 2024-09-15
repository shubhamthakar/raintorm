# distributed-logger
Group No. 69 (sthakar3 and chaskar2)

We have created a distributed logger on a cluster of 10 VMs. Each of the 10 machines has a server running listening for socket connections. A client socket can be created on any of the 10 machines. The client will parallelly open connections to the server and run grep command on each machine. We have used Thread Pooling to enable parallel execution. The grep output will then be passed on to the client where it is displayed on the terminal. 

# Setup

1. Pull latest code

    In order to have the latest code base on all the machines, execute the `git_cloner.sh` utility script.

    ```shell
    cd /home/chaskar2/
    sh git_cloner.sh
    ```

2. Run Socket Server

    To run the socket server across all the 10 machine, execute the `socket_server_runner.sh` utility script. This will start server sockets on all the machines

    ```shell
    cd /home/chaskar2/
    sh socket_server_runner.sh
    ```

3. Run grep using Socket clients

    To run the grep commands across all the log files, execute the `socket_client_parallel.py` with the pattern to be grepped.

    ```shell
    cd /home/chaskar2/distributed-logger
    python socket_client_parallel.py -r 'http://reynolds.com/' /home/chaskar2/log/
    ```

4. Cleanup

    To stop the socket servers running accross all the amchines, execute the `socket_server_cleanup.sh`

    ```shell
    cd /home/chaskar2/
    sh socket_server_cleanup.sh
    ```


