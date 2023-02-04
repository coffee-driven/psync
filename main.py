#!/usr/bin/env python3

import argparse
import logging
import os
import re
import time

from fabric import Connection
from paramiko import client

from multiprocessing import Process, Queue, Event
from queue import Empty


class HostConnection():
    """Connection object"""
    def __init__(self, host: str, port: int, username: str, private_key: str) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.private_key = private_key
        self.connection = object

    def open_connection(self) -> object:
        print("opening")
        self.connection = client.SSHClient()
        self.connection.set_missing_host_key_policy(client.AutoAddPolicy())
        try:
            self.connection.connect(hostname=self.host, username=self.username, port=self.port, key_filename=self.private_key)
        except Exception as e:
            print(e)
            return None

        # Somehow I don't need to set it when the connection is passed to command, maybe it happens automatically
        # Should solve problem with shared socket between parent and child processes, shall create new socket at fork
        # transport = self.connection.get_transport()
        # transport.atfork()
        con = self.connection
        print("opened")
        return con

    def close_connection(self):
        self.connection.close()


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
            return None
        return connection_pool


class RemoteCommands:
    """
       Remote command object.
       Data queue is used for control data - sizes, hashes
       Management queue is used for siginalization
    """
    def __init__(self, connection: HostConnection, data_queue: Queue, host_id: str):
        Process.__init__(self)
        self.connection = connection
        self.data_queue = data_queue
        self.id = host_id

    def calculate_file_hash(self, status: Event, status_struct: dict) -> dict:
        """Calculate hash of file, files are sorted by size"""
        print("Calculating file hash")
        files = []
        files = self.data_queue.get()
        print("Get from queue")
        print(files)

        print(files)

        for i in files:
            print(i)
        file_paths = ' '.join(files)
        print("here")
        print(file_paths)

        cmd = 'echo {}'.format(file_paths)

        c = self.connection.open_connection()
        _, sout, _ = c.exec_command(cmd)
        checksums_files = sout.readlines()
        time.sleep(3)
        print(checksums_files)
        return

        data = {}
        for checksum_file in checksums_files:
            listed = re.split("\t", checksum_file)
            checksum = str(listed[0])
            file = str(listed[1]).rstrip("\n")
            data[file] = checksum

        self.data_queue.put({self.id: data})


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

    def remote_sleep(self, event: Event):
        print("Remote sleep command")
        c = self.connection.open_connection()
        sin, sout, serr = c.exec_command("sleep 1 && echo Up!")
        out = sout.readlines()
        time.sleep(1)
        self.data_queue.put([out])
        event.set()

    def get_files_and_sizes(self, files: list) -> dict:
        """Find files, get their size return list sorted by size."""
        print("Remote command get files and sizes")
        c = self.connection.open_connection()
        file_paths = ' '.join(files)
        cmd = 'for i in {} ; do find $i ; done'.format(file_paths)
        sin, sout, serr = c.exec_command(cmd)
        out = sout.readlines()

        all_files = ' '.join(out)
        cmd = 'du {}'.format(all_files)
        sin, sout, serr = c.exec_command(cmd)
        sizes_files = sout.readlines()

        print("CMD finished")

        data = {}
        for size_file in sizes_files:
            listed = re.split("\t", size_file)
            size = int(listed[0])
            file = str(listed[1]).rstrip("\n")
            data[file] = size

        sorted_by_size = dict(sorted(data.items(), key=lambda x: x[1]))
        print("Putting to queue")
        print(sorted_by_size)
        self.data_queue.put({self.id: sorted_by_size})


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

    def get_files(self, host):
        files = list(self.config[host]['files'].keys())
        return files

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
    def __init__(self, scheduler_configuration: dict, configuration: dict, config_parser: object) -> None:
        self.config = configuration
        self.connection_pool = {}
        self.remote_files = {}
        self.local_files = {}
        self.max_parallel_conns = scheduler_configuration['max_parallel_connections']
        self.hosts = []
        self.private_key = str

        self.config_parser = config_parser(self.config)

        self.hosts = self.config_parser.get_hosts()

    def get_local_files_and_sizes(self):
        pass

    def run(self):

        global_status = {}
        first_stage_counter = 0
        first_stage_queue = Queue()
        process_list = []
        second_stage_queues = []
        status_update_event = Event()

        for host in self.hosts:
            print("FIRST RUN")
            conn_config = {
                'host': self.config_parser.get_address(host),
                'port': self.config_parser.get_port(host),
                'private_key': self.config_parser.get_private_key(host),
                'connections': self.config_parser.get_connections(host),
                'username': self.config_parser.get_username(host),
                }

            host_connection_pool = HostConnectionPool(conn_config)
            connection_pool = host_connection_pool.initialize_pool()
            connections = [x for x in connection_pool]
            self.connection_pool[host] = connections

            global_status[host] = {}

            files = self.config_parser.get_files(host)
            connector = self.connection_pool[host][0]

            remote_command = RemoteCommands(connection=connector, data_queue=first_stage_queue, host_id=host)
            files_and_sizes_sorted = remote_command.get_files_and_sizes
            # local_files_and_sizes = remote_command.get_local_files_and_sizes

            print("Check files and sizes")
            remote_files_processing = Process(target=files_and_sizes_sorted, args=(files,))
            remote_files_processing.start()

            # Create private queue for host
            print("Create host queue")
            q = Queue()
            second_stage_queues.append(q)
            print(second_stage_queues)

        first_stage_counter = 1
        while True:
            if first_stage_counter == len(self.hosts):
                break
            else:
                first_stage_counter += 1

            files_sizes = first_stage_queue.get()
            print("Getting from queue first stage")
            print(files_sizes)

            hostname = list(files_sizes.keys())[0]
            for _, values in files_sizes.items():
                files = values

            print("Create download list")
            download_list = [str(filename) for filename in files.keys()]
            print(download_list)
            flow_queue = second_stage_queues.pop()

            global_status[hostname] = {
                'size_and_files': 'OK',
                'max_tries': 3,
                'tries': 0,
            }

            print("Putting list to host queue")
            flow_queue = second_stage_queues.pop()
            flow_queue.put(download_list)

            checksum_command = RemoteCommands(connection=connector, data_queue=flow_queue, host_id=host)
            remote_checksum = checksum_command.calculate_file_hash
            remote_checksum_calculation_process = Process(target=remote_checksum, args=(status_update_event, global_status))

         #   get_command = RemoteCommands(connection=connector, data_queue=flow_queue, host_id=host)
         #   get_file = get_command.get_file
         #   get_file_process = Process(target=get_file)
#
         #   check_downloaded_file = Process(target=check_downloaded_file, args=(flow_queue, global_status))
#
            remote_checksum_calculation_process.start()
         #   get_file_process.start()
         #   check_downloaded_file.start()
#
            process_list.append(remote_checksum_calculation_process)
         #   process_list.append(get_file_process)
         #   process_list.append(check_downloaded_file)
            remote_checksum_calculation_process.join()


        # Check status, if done, close queues, terminate processes, update status
        
        print(global_status)

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

    scheduler_config = {
        'max_parallel_connections': 100,
    }

    config = {
        "vm1": {
            "host": "127.0.0.1",
            "port": 2022,
            "username": "bob",
            "private_key": args.private_key,
            "default_storage": "/tmp",
            "connections": 3,
            "files": {
                "/home/testfile": {
                    "sync_options": "options",
                    "local_path": "local_path"
                },
            },
        },
        "vm2": {
            "host": "127.0.0.1",
            "port": 2022,
            "username": "bob",
            "private_key": args.private_key,
            "default_storage": "/tmp",
            "connections": 3,
            "files": {
                "/home/testfile": {
                    "sync_options": "options",
                    "local_path": "local_path"
                },
            },
        },
    }

    scheduler = Scheduler(scheduler_configuration=scheduler_config, configuration=config, config_parser=ConfigParser)
    scheduler.run()
    exit()

    config = {}
    cfg = {}
    while True:
        cfg = read_configuration()
        if cfg != config and cfg != None:
            config = cfg
            Scheduler.reload(configuration=config)


if __name__ == "__main__":
    main()
