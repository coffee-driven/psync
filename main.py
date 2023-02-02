#!/usr/bin/env python3

import argparse
import logging
import os
import time

from fabric import Connection
from paramiko import client

from multiprocessing import Process, Queue, Event


class HostConnection():
    """Connection object"""
    def __init__(self, host: str, port: int, username: str, private_key: str) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.private_key = private_key
        self.connection = object

    def open_connection(self) -> object:
        self.connection = client.SSHClient()
        self.connection.set_missing_host_key_policy(client.AutoAddPolicy())
        try:
            self.connection.connect(hostname=self.host, username=self.username, port=self.port, key_filename=self.private_key)
        except Exception as e:
            print(e)
            return None

        transport = self.connection.get_transport()
        transport.atfork()

    def close_connection(self):
        self.connection.close()

    def sleep(self, q, signal):
        print("Sleeping")
        self.connection.connect(hostname=self.host, username=self.username, port=self.port, key_filename=self.private_key)
        sin, sout, serr = self.connection.exec_command("sleep 1 && ls /")
        out = sout.readlines()
        q.put([self.host, out])
        signal.set()
        print("Woken up!")


class HostConnectionPool():
    def __init__(self, config: dict) -> None:
        self.host = config['host']
        self.port = config['port']
        self.username = config['username']
        self.private_key = config['private_key']
        self.connections = config['connections']

    def initialize_pool(self):
        logging.debug("Initializing pool")
        # TODO: Implement interface
        connection_pool = [HostConnection(self.host, self.port, self.username, self.private_key) for _ in range(0, self.connections, 1)]
        for c in connection_pool:
            try:
                c.open_connection()
            except Exception as e:
                print("Connection doesn't work. Removing from list")
                print(e)
                connection_pool.remove(c)

        if not connection_pool:
            logging.error("Connection pool is empty  for host %s", self.conf_cfg["host"])
            return
        return connection_pool


class RemoteCommands:
    """
       Remote command object.
       Data queue is used for control data - sizes, hashes
       Management queue is used for siginalization
    """
    def __init__(self, connection: HostConnection, data_queue: Queue, management_queue: Queue):
        self.connection = connection
        self.data_queue = data_queue
        self.management_queue = management_queue

    def check_sender_script(self):
        """Return checksum of script or none"""
        try:
            self.connection.get("/tmp/sender.py")
            return self.connection.run("md5sum /tmp/sender.py")
        except Exception as e:
            print("Remote synchronization script is missing")
            raise FileNotFoundError(e)

    def remote_agent(self):
        """
        Run in parallel with synchronization.
        1. Create dict of files sorted by size
        2. Launch multiple processes for various group of file sizes
        3. Fill the message bus dict of file ready to download - checksum
        """

    def synchronize(self, files_and_options: dict):
        print('{}: {}'.format(self.host, "Synchronizing files"))

        for file in files_and_options:
            print('{}: {} {}'.format(self.host, "Synchronizing file", file['path']))

            try:
                local_path = file['local_path']
            except KeyError:
                local_path = self.local_storage

            try:
                self.connection.get(file['path'], local=local_path)
            except FileNotFoundError:
                print("File not found")
            except PermissionError:
                print("File is inaccessible")

    def sync_sender_script(self):
        print("Copying remote synchronization script")
        pass

    def sleep(self, q):
        print("Sleeping")
        self.connection.connect(hostname=self.host, username=self.username, port=self.port, key_filename=self.private_key)
        sin, sout, serr = self.connection.exec_command("sleep 1 && ls /")
        out = sout.readlines()
        q.put([self.host, out])
        print("Woken up!")

    def find_file_size(self) -> dict:
        """Find files and their size"""

    def calculate_file_hash(self) -> dict:
        """Calculate hash of file, files are sorted to groups small, medium and large, each group is processed as separate process."""


class ConfigParser:
    def __init__(self, config: dict):
        self.config = config

        self.default_port = 22
        self.default_connections = 1
        self.default_username = 'psync'

    def get_address(self, host):
        try:
            addr = self.config[host]['host']
            # Maybe check type and sanitize
            return addr
        except KeyError:
            return None

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



class Scheduler:
    """
        Take files, syncing option and schedule synchronization subprocesses.
    """
    def __init__(self, configuration: dict, config_parser: object) -> None:
        self.config = configuration
        self.connection_pool = []
        self.remote_files = {}
        self.local_files = {}
        self.hosts = []
        self.private_key = str

        self.config_parser = config_parser(self.config)

        self.hosts = self.config_parser.get_hosts()

    #def get_files_and_sizes(self):
    #    # find files, get their size and update data struct
    #    for host, options in self.config:
    #        for file in options['files']:
    #        # file is list
    #        # O(nm), using dict in nested for it can be O(n)
    #            # result = connection exec find -exec du
    #        # Update self.config[host][files] = result

    def get_local_files_and_sizes(self):
        pass

    def run(self):
        for host in self.hosts:
            config = {
                'host': self.config_parser.get_address(host),
                'port': self.config_parser.get_port(host),
                'private_key': self.config_parser.get_private_key(host),
                'connections': self.config_parser.get_connections(host),
                'username': self.config_parser.get_username(host),
                }

            host_connection_pool = HostConnectionPool(config)
            connection_pool = host_connection_pool.initialize_pool()
            [self.connection_pool.append(x) for x in connection_pool]
            del(host_connection_pool)

        print(self.connection_pool)

    def reload(self, configuration):
        pass

    def synchronize(self):
        # sort files, synchronize
        sorted_files_and_sizes = dict(sorted(files_and_sizes.items(), key=lambda x: x[1]))
        small_files, medium_files, big_files = group_files_by_size(sorted_files_and_sizes)

        event = Event()
        queue = Queue()
        prcs = []
        for host_con in connection_pool:
            p = Process(target=host_con.sleep, args=(queue,event))
            p.start()
            prcs.append(p)


        print("getting from queue")
        while True:
            event.wait()
            print(q.get())

        for i in range(1, 4, 1):
            print(q.get())

        [x.join() for x in prcs]


def main():
    parser = argparse.ArgumentParser(
                    prog = 'ProgramName',
                    description = 'What the program does',
                    epilog = 'Text at the bottom of help')
    parser.add_argument('--remote_path', dest='remote_path', help='Local path')
    parser.add_argument('--private_key', dest='private_key', action='store', help='Private key', required=True) 

    args = parser.parse_args()

    config = {
        "vm1": {
            "host": "127.0.0.1",
            "port": 2022,
            "username": "bob",
            "private_key": args.private_key,
            "default_storage": "/tmp",
            "files": {
                "path_or_file": {
                    "sync_options": "options",
                    "local_path": "local_path"
                },
            },
        },
    }



    scheduler = Scheduler(configuration=config, config_parser=ConfigParser)
    scheduler.run()
    exit()

    config = {}
    cfg = {}
    while True:
        cfg = read_configuration()
        if cfg != config and cfg != None:
            config = cfg
            Scheduler.reload(configuration=config)
    
        
        for host in config:
            connection_config = host['connection_config']
            Connections = host['connections']
            files_and_options = host['files_and_options']
            
            host_connection_pool = HostConnectionPool(connection_config=config, connections=3)
            connection_pool = host_connection_pool.initialize_pool()

        scheduler = Scheduler(files_and_options, connection_pool)

        remote_files_processing = Process(target=scheduler.get_files_and_sizes)
        local_files_processing = Process(target=scheduler.get_local_files_and_sizes)

        remote_files_processing.start()
        local_files_processing.start()

        remote_files_processing.join()
        local_files_processing.join()

        scheduler.synchronize()


main()
