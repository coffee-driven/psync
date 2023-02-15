from fabric import Connection
from psync import psync


def test_con():
    host_connection = psync.HostConnection('127.0.0.1', 2022, "bob", "/home/m/Documents/GitHub/psync/priv")
    assert isinstance(host_connection.open_connection(), Connection)

def test_con_pool_init():
    host_con_pool = psync.HostConnectionPool({
            'host': '127.0.0.1',
            'port': 2022,
            'private_key': "/home/m/Documents/GitHub/psync/priv",
            'connections': 3,
            'username': "bob",
            })
    pool = host_con_pool.initialize_pool()
    res = map(lambda x: isinstance(x, psync.HostConnection), pool)
    for is_connection in res:
        assert is_connection
