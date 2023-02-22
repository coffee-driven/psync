class ConfigParser:
    def __init__(self, config: dict):
        self.config = config

        self.default_port = 22
        self.default_connections = 3
        self.default_username = 'psync'

    def get_address(self, host):
        try:
            addr = self.config[host]['host']
            # Maybe check type and sanitize
            return addr
        except KeyError:
            return None

    def get_connection_config(self, host):
        config = {
            'host': self.get_address(host),
            'port': self.get_port(host),
            'private_key': self.get_private_key(host),
            'connections': self.get_connections(host),
            'username': self.get_username(host),
            }
        return config

    def get_files(self, host):
        files = list(self.config[host]['files'].keys())
        return files

    def get_files_local_storage(self, host, path):
        local_storage = str(self.config[host]["files"][path]["local_path"])
        return local_storage

    def get_port(self, host):
        try:
            port = self.config[host]['port']
            # Maybe check type and sanitize
            return port
        except KeyError:
            return self.default_port

    def get_private_key(self, host):
        try:
            private_key = self.config[host]['private_key']
            return private_key
        except KeyError:
            return None

    def get_connections(self, host):
        try:
            connections = self.config[host]['connections']
            return connections
        except KeyError:
            return self.default_connections

    def get_username(self, host):
        try:
            username = self.config[host]['username']
            return username
        except KeyError:
            return self.default_username

    def get_hosts(self) -> list:
        hosts = list(self.config.keys())
        return hosts
