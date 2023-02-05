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


def logger():
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    return logger


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


class LocalCommands:
    def __init__(self, data_in: Queue, data_out: Queue):
        self.data_in = data_in
        self.data_out = data_out
        self.logger = logger()

    def check_local_file(self):
        files = {}
        while True:
            try:
                files = self.data_in.get()
            except Empty:
                pass

            if files[0] is None:
                return
        # Logic to get file, hash, compare output
            self.data_out.put({'file': True})



class RemoteCommands:
    """
       Remote command object.
       Data queue is used for control data - sizes, hashes
       Management queue is used for siginalization
    """
    def __init__(self, connection: HostConnection, data_in: Queue, data_out: Queue):
        self.connection = connection
        self.data_in = data_in
        self.data_out = data_out
        self.logger = logger()

    def calculate_file_hash(self) -> dict:
        """Calculate hash of file, files are sorted by size"""
        self.logger.info("Calculating file hash")
        files = []
        while True:
            try:
                files = self.data_in.get(block=False, timeout=0.5)
            except Empty:
                continue

            if files[0] is None:
                return

            self.logger.debug("Get from queue")

            for i in files:
                print(i)
            file_paths = ' '.join(files)
            print("here")
            print(file_paths)

            cmd = 'md5sum {}'.format(file_paths)

            c = self.connection.open_connection()
            _, sout, _ = c.exec_command(cmd)
            checksums_files = sout.readlines()
            time.sleep(3)
            self.logger.debug("Checksum gained")

            for checksum_file in checksums_files:
                # md5sum splits the data by two spaces (Debian10)
                listed = re.split("  ", checksum_file)
                checksum = str(listed[0])
                file = str(listed[1]).rstrip("\n")

                self.logger.debug("Putting data on queue")
                self.data_out.put({file: checksum})

    def get_file(self):
        c = self.connection.open_connection()

        while True:
            try:
                files = self.data_in.get(block=False, timeout=0.2)
            except Empty:
                continue

            if files[0] is None:
                return

            for file in files:
                self.logger.info("Downloading file %s", file)
                self.data_out.put({file: True})

    def remote_sleep(self, event: Event):
        print("Remote sleep command")
        c = self.connection.open_connection()
        sin, sout, serr = c.exec_command("sleep 1 && echo Up!")
        out = sout.readlines()
        time.sleep(1)
        self.data_queue.put([out])
        event.set()

    def get_files_and_sizes(self) -> dict:
        """Find files, get their size return list sorted by size."""
        print("Remote command get files and sizes")
        c = self.connection.open_connection()

        files = self.data_in.get()
        file_paths = ' '.join(files)
        cmd = 'for i in {} ; do find $i ; done'.format(file_paths)
        sin, sout, serr = c.exec_command(cmd)
        out = sout.readlines()

        all_files = ' '.join(out)
        cmd = 'du {}'.format(all_files)
        sin, sout, serr = c.exec_command(cmd)
        sizes_files = sout.readlines()

        self.logger.debug("Files and sizes finished")

        data = {}
        for size_file in sizes_files:
            listed = re.split("\t", size_file)
            size = int(listed[0])
            file = str(listed[1]).rstrip("\n")
            data[file] = size

        sorted_by_size = dict(sorted(data.items(), key=lambda x: x[1]))
        self.logger.debug("Files and sizes: Putting to queue")
        self.data_out.put(sorted_by_size)


class Scheduler:
    """
        Take files, syncing option and schedule synchronization subprocesses.
    """
    def __init__(self, scheduler_configuration: dict, configuration: dict, config_parser: object, reload: Event) -> None:
        self.config = configuration
        self.connection_pool = {}
        self.remote_files = {}
        self.local_files = {}
        self.max_parallel_conns = scheduler_configuration['max_parallel_connections']
        self.hosts = []
        self.logger = logger()
        self.private_key = str
        self.reload = reload

        self.config_parser = config_parser(self.config)

        self.hosts = self.config_parser.get_hosts()

    def get_local_files_and_sizes(self):
        pass

    def run(self):
        global_status = {}
        first_stage_counter = 0
        first_stage_queue_out = Queue()
        second_stage_queue_out = Queue()
        reload_event = Event()
        result = []

        # First stage
        for host in self.hosts:
            self.logger.debug("Iterating hosts")
            conn_config = {
                'host': self.config_parser.get_address(host),
                'port': self.config_parser.get_port(host),
                'private_key': self.config_parser.get_private_key(host),
                'connections': self.config_parser.get_connections(host),
                'username': self.config_parser.get_username(host),
                }

            global_status[host] = {}
            files = self.config_parser.get_files(host)

            host_connection_pool = HostConnectionPool(conn_config)
            connection_pool = host_connection_pool.initialize_pool()
            connections = [x for x in connection_pool]

            input = Queue()
            remote_command = RemoteCommands(connection=connections[0], data_in=input, data_out=first_stage_queue_out)
            files_and_sizes_sorted = remote_command.get_files_and_sizes
            # local_files_and_sizes = remote_command.get_local_files_and_sizes

            self.logger.debug("Check files and sizes")
            remote_files_processing = Process(target=files_and_sizes_sorted)
            remote_files_processing.start()
            input.put(files)

        # Second stage
        first_stage_counter = 0
        while True:
            if first_stage_counter == len(self.hosts):
                break

            self.logger.debug("Getting from queue first stage")
            try:
                files_sizes = first_stage_queue_out.get(block=False, timeout=0.2)
            except Empty:
                continue
            else:
                first_stage_counter += 1

            result.append(files_sizes)

            host_name = list(files_sizes.keys())[0]

            self.logger.debug("Create download list")
            download_list = []
            for filename, _ in files_sizes.items():
                download_list.append(filename)

            global_status[host_name] = {
                'size_and_files': 'OK',
                'max_download_tries': 3,
                'files': {},
            }

            pipeline_queue_in = Queue()
            pipeline = Pipeline(pipeline_queue_in, second_stage_queue_out, host_name, connection_pool, reload_event)
            command_pipeline = pipeline.command_pipeline
            pipeline_process = Process(target=command_pipeline)
            pipeline_process.start()
            pipeline_queue_in.put(download_list)

        # Consume output of second stage
        res = ""
        while True:
            self.logger.debug("main loop")
            if self.reload.is_set():
                self.logger.info("Scheduler reloading")
                reload_event.set()
                pipeline_process.join()
                return

            try:
                res = second_stage_queue_out.get(timeout=0.3)
            except Empty:
                if not pipeline_process.is_alive():
                    break
                time.sleep(0.3)
            else:
                result.append(res)

        while True:
            try:
                res = second_stage_queue_out.get(timeout=0.3)
            except Empty:
                break
            else:
                result.append(res)

        print(result)
        return


class Pipeline:
    def __init__(self, data_in: Queue, data_out: Queue, host_id: str, connections: HostConnectionPool, reload: Event) -> None:
        self.connections = connections
        self.data_in = data_in
        self.data_out = data_out
        self.host_id = host_id
        self.logger = logger()
        self.reload = reload

    def command_pipeline(self):
        self.logger.info("Pipeline launched")
        filenames = self.data_in.get()
        filenames_ok = 0

        checksum_queue_in = Queue()
        checksum_queue_out = Queue()
        checksum_command = RemoteCommands(connection=self.connections[0], data_in=checksum_queue_in, data_out=checksum_queue_out)
        remote_checksum = checksum_command.calculate_file_hash
        remote_checksum_calculation_process = Process(target=remote_checksum)
        remote_checksum_calculation_process.start()
        checksum_queue_in.put(filenames)

        download_queue_in = Queue()
        download_queue_out = Queue()
        get_command = RemoteCommands(connection=self.connections[1], data_in=download_queue_in, data_out=download_queue_out)
        get_file = get_command.get_file
        get_file_process = Process(target=get_file)
        get_file_process.start()

        local_check_queue_in = Queue()
        local_check_queue_out = Queue()
        local_command = LocalCommands(data_in=local_check_queue_in, data_out=local_check_queue_out)
        local_command_check_file = local_command.check_local_file
        local_check_command_process = Process(target=local_command_check_file)
        local_check_command_process.start()

        while True:
            self.logger.debug("Pipeline loop")
            if self.reload.is_set():
                self.logger.info("Pipeline terminating command processes")
                checksum_queue_in.put([None])
                download_queue_in.put([None])
                local_check_queue_in.put([None])

                return

            self.logger.debug("Check checksum queue")
            try:
                file = checksum_queue_out.get(block=False, timeout=0.2)
            except Empty:
                pass
            else:
                for k, v in file.items():
                    filename = k
                    hash = v
                self.data_out.put({filename: hash})
                self.logger.debug("putting on dload queue")
                download_queue_in.put([filename])

            self.logger.debug("Checking dload queue")
            try:
                dloaded_file = download_queue_out.get(block=False, timeout=0.2)
            except Empty:
                pass
            else:
                self.logger.debug("Putting on check queue")
                local_check_queue_in.put([dloaded_file])

            self.logger.debug("Checking finall status")
            try:
                dload_status = local_check_queue_out.get(block=False, timeout=0.2)
            except Empty:
                pass
            else:
                # Implement checks and retries
                self.logger.debug("File downloaded")
                for filename, ok in dload_status.items():
                    if not ok:
                        self.logger.debug("File is corrupted %s", filename)
                        download_queue_in.put(filename)
                    else:
                        self.logger.debug("File is ok")
                        filenames_ok += 1
                        self.data_out.put({self.host_id: filename})

            if len(filenames) == filenames_ok:
                # Poison pill
                self.logger.info("Terminating pipeline subprocesses")
                checksum_queue_in.put([None])
                download_queue_in.put([None])
                local_check_queue_in.put([None])

                return


def main():

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
                "/home/testfile2": {
                    "sync_options": "options",
                    "local_path": "local_path"
                },
            },
        },
    }

    # Write config checker
    # Write manager that will receive notifications from config checker and reload scheduler.
    # Manager shall be accessible through the API (maybe Flask)
    reload_scheduler = Event()
    scheduler = Scheduler(scheduler_configuration=scheduler_config, configuration=config, config_parser=ConfigParser, reload=reload_scheduler)
    schedule = scheduler.run
    process = Process(target=schedule)
    process.start()
    #reload_scheduler.set()
    process.join()
    exit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
                    prog = 'ProgramName',
                    description = 'What the program does',
                    epilog = 'Text at the bottom of help')
    parser.add_argument('--remote_path', dest='remote_path', help='Local path')
    parser.add_argument('--private_key', dest='private_key', action='store', help='Private key', required=True)

    args = parser.parse_args()

    main()
