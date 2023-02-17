from fabric import Connection
from multiprocessing import Process, Queue
from psync import psync


def test_con():
    host_connection = psync.HostConnection('127.0.0.1', 2022, "bob", "/home/m/Documents/GitHub/psync/docker/priv")
    assert isinstance(host_connection.open_connection(), Connection)

def test_con_pool_init():
    host_con_pool = psync.HostConnectionPool({
            'host': '127.0.0.1',
            'port': 2022,
            'private_key': "/home/m/Documents/GitHub/psync/docker/priv",
            'connections': 3,
            'username': "bob",
            })
    pool = host_con_pool.initialize_pool()
    res = map(lambda x: isinstance(x, psync.HostConnection), pool)
    for is_connection in res:
        assert is_connection

def test_files_and_sizes_parser():
    test_data = ['99\tmy_file']
    res = psync.RemoteCommands.parse_files_and_sizes(test_data)
    size = res["my_file"]
    assert size is 99

def test_remote_cmd_file_and_sizes():
    test_data = ['/home/testfile', '/home/absent_file']
    host_connection = psync.HostConnection('127.0.0.1', 2022, "bob", "/home/m/Documents/GitHub/psync/docker/priv")
    q_in = Queue()
    q_out = Queue()

    q_in.put(test_data)
    cmd = psync.RemoteCommands(connection=host_connection, data_in=q_in, data_out=q_out)
    p = Process(target=cmd.get_files_and_sizes)
    p.start()
    res = q_out.get(timeout=10)
    p.terminate()
    
    found = res["found"]
    not_found = res["not_found"]
    present_file_size = found["/home/testfile"]
    absent_file_size = not_found["/home/absent_file"]

    assert int(present_file_size) > 0
    assert int(absent_file_size) == -1

