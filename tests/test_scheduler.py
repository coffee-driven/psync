from fabric import Connection
from multiprocessing import Process, Queue
from psync import conn, commands, config_parser as conf_parser


def test_con():
    host_connection = conn.HostConnection('127.0.0.1', 2022, "bob", "/home/m/Documents/GitHub/psync/docker/priv")
    assert isinstance(host_connection.open_connection(), Connection)


def test_con_pool_init():
    host_con_pool = conn.HostConnectionPool({
            'host': '127.0.0.1',
            'port': 2022,
            'private_key': "/home/m/Documents/GitHub/psync/docker/priv",
            'connections': 3,
            'username': "bob",
            })
    pool = host_con_pool.initialize_pool()
    res = map(lambda x: isinstance(x, conn.HostConnection), pool)
    for is_connection in res:
        assert is_connection


def test_files_and_sizes_parser():
    test_data = ['99\tmy_file']
    res = commands.RemoteCommands.parse_files_and_sizes(test_data)
    size = res["my_file"]
    assert size == 99


def test_remote_cmd_file_and_sizes():
    test_data = ['/home/testfile', '/home/absent_file']
    host_connection = conn.HostConnection('127.0.0.1', 2022, "bob", "/home/m/Documents/GitHub/psync/docker/priv")
    q_in = Queue()
    q_out = Queue()

    q_in.put(test_data)
    cmd = commands.RemoteCommands(connection=host_connection, data_in=q_in, data_out=q_out)
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


def test_remote_cmd_file_hash():
    host_connection = conn.HostConnection('127.0.0.1', 2022, "bob", "/home/m/Documents/GitHub/psync/docker/priv")
    test_data = ["/home/testfile", "/home/testfile2"]
    q_in = Queue()
    q_out = Queue()

    q_in.put(test_data)
    cmd = commands.RemoteCommands(connection=host_connection, data_in=q_in, data_out=q_out)
    p = Process(target=cmd.calculate_file_hash)
    p.start()
    res = q_out.get(timeout=10)
    p.terminate()

    for filename, hash in res.items():
        assert filename == "/home/testfile"
        assert hash == "b026324c6904b2a9cb4b88d6d61c81d1"


def test_file_download():
    host_connection = conn.HostConnection('127.0.0.1', 2022, "bob", "/home/m/Documents/GitHub/psync/docker/priv")
    remote_path = "/home/testfile"
    local_path = "/tmp"
    local_store = "{}/{}".format(local_path, remote_path)
    test_data = [(remote_path, local_path),]
    q_in = Queue()
    q_out = Queue()

    q_in.put(test_data)
    cmd = commands.RemoteCommands(connection=host_connection, data_in=q_in, data_out=q_out)
    p = Process(target=cmd.get_file)
    p.start()
    res = q_out.get(timeout=10)
    p.terminate()

    for k, v in res.items():
        assert k == remote_path
        assert v is True


def test_local_check():
    test_data = ("/home/testfile", "/tmp")
    q_in = Queue()
    q_out = Queue()
    cmd = commands.LocalCommands(data_in=q_in, data_out=q_out)
    p = Process(target=cmd.get_local_checksum)

    p.start()
    q_in.put([test_data])
    res = q_out.get(timeout=5)
    p.terminate()

    for k, v in res.items():
        assert v["local_path"] == "/tmp/home/testfile"
        assert v["checksum"] == "b026324c6904b2a9cb4b88d6d61c81d1"


def test_get_files_local_path():
    cfg = {"vm2": {
            "host": "127.0.0.2",
            "port": 2022,
            "username": "bob",
            "private_key": "",
            "storage": "/tmp",
            "connections": 3,
            "files": {"/home/testfile2"}
            }}

    test_data = "/home/testfile2"

    config_parser = conf_parser.ConfigParser(cfg)
    local_store = config_parser.get_files_local_storage("vm2", test_data)

    assert local_store == "/tmp"
