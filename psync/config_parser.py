import logging


def logger():
    logger = logging.getLogger()
    return logger


class ConfigParser:
    def __init__(self, config: dict):
        self.config = config
        self.logger = logger()

        self.default_port = 22
        self.default_connections = 3
        self.default_username = 'psync'

    def validate(self):
        mandatory_params = { "files": list,
                             "host": str,
                             "private_key": str,
                             "username": str,
                             "storage": str}

        for host_conf in self.config.values():
            self.__validator(host_conf, mandatory_params)

    def __validator(self, config, params) -> None:
        for key, type in params.items():
            try:
                val = config[key]
            except KeyError:
                self.logger.error("Configuration error. Missing configuration option: %s", key)
                exit(1)
            else:
                if not isinstance(val, type):
                    self.logger.error("Configuration error. Wrong value type for option: %s", val)
                    exit(1)

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
        files = list(self.config[host]["files"])
        return files

    def get_files_local_storage(self, host, path):
        local_storage = str(self.config[host]["storage"])
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
