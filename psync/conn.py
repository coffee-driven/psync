import logging

from fabric import Connection


def logger():
    logger = logging.getLogger()
    return logger


class HostConnection():
    """Connection object"""
    def __init__(self, host: str, port: int, username: str, private_key: str) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.private_key = private_key
        self.connection = object
        self.logger = logger()

    def open_connection(self) -> object:

        self.logger.debug("Opening connection to host")
        self.connection = Connection(host=self.host,
                                     user=self.username,
                                     port=self.port,
                                     connect_kwargs={"key_filename": self.private_key,},)
        self.connection.open()

        con = self.connection
        return con
        """
        self.connection = client.SSHClient()
        self.connection.set_missing_host_key_policy(client.AutoAddPolicy())
        try:
            self.connection.connect(hostname=self.host, username=self.username, port=self.port, key_filename=self.private_key)
        except Exception as e:
            self.logger.error(e)
            return None

        con = self.connection
        self.logger.debug("opened")
        return con
        """
    def close_connection(self):
        self.connection.close()


class HostConnectionPool():
    def __init__(self, config: dict) -> None:
        self.host = config['host']
        self.port = config['port']
        self.username = config['username']
        self.private_key = config['private_key']
        self.connections = config['connections']
        self.logger = logger()

    def initialize_pool(self):
        self.logger.debug("Initializing pool")
        # TODO: Implement interface
        connection_pool = [HostConnection(self.host, self.port, self.username, self.private_key) for _ in range(0, self.connections, 1)]
        for c in connection_pool:
            try:
                c.open_connection()
            except Exception as e:
                self.logger.error("Connection doesn't work. Removing from list %s", e)
                connection_pool.remove(c)
            else:
                c.close_connection()

        if not connection_pool:
            self.logging.error("Connection pool is empty for host %s", self.conf_cfg["host"])
            return None
        return connection_pool
