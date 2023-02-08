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
    logger.setLevel(logging.INFO)

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
        self.logger = logger()

    def initialize_pool(self):
        self.logger.debug("Initializing pool")
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
            self.logging.error("Connection pool is empty  for host %s", self.conf_cfg["host"])
            return None
        return connection_pool


class LocalCommands:
    def __init__(self, data_in: Queue, data_out: Queue):
        self.data_in = data_in
        self.data_out = data_out
        self.logger = logger()

    def check_local_file(self):
        self.logger.info("cmd check local file launched")
        files = {}
        while True:
            try:
                files = self.data_in.get()
            except Empty:
                pass

            if files[0] is None:
                self.logger.info("cmd check local file finished")
                return
        # Logic to get file, hash, compare output
            self.logger.info("cmd check local file putting on queue")
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
        self.logger.info("cmd calculate file hash launched")
        files = []
        while True:
            try:
                files = self.data_in.get(block=False, timeout=0.5)
            except Empty:
                continue
            if files[0] is None:
                self.logger.info("cmd calculate file hash finished")
                return

            self.logger.debug("received item from queue")

            for i in files:
                print(i)
            file_paths = ' '.join(files)

            cmd = 'md5sum {}'.format(file_paths)

            c = self.connection.open_connection()
            _, sout, _ = c.exec_command(cmd)
            checksums_files = sout.readlines()
            time.sleep(1)
            self.logger.debug("Checksum gained")

            for checksum_file in checksums_files:
                # md5sum splits the data by two spaces (Debian10)
                listed = re.split("  ", checksum_file)
                checksum = str(listed[0])
                file = str(listed[1]).rstrip("\n")

                self.logger.info("cmd calculate file has putting on queue")
                self.data_out.put({file: checksum})

    def get_file(self):
        self.logger.info("cmd get file launched")
        c = self.connection.open_connection()

        while True:
            try:
                files = self.data_in.get(block=False, timeout=0.2)
            except Empty:
                continue

            if files[0] is None:
                self.logger.info("cmd get file finished")
                return

            for file in files:
                self.logger.info("cmd get file downloading file %s", file)
                self.data_out.put({file: True})

    def remote_sleep(self, event: Event):
        self.logger.info("cmd remote sleel launched")
        c = self.connection.open_connection()

        sin, sout, serr = c.exec_command("sleep 1 && echo Up!")
        
        out = sout.readlines()
        time.sleep(1)
        
        self.logger.info("cmd remote command putting on queue")
        self.data_queue.put([out])
        event.set()

    def get_files_and_sizes(self) -> dict:
        """Find files, get their size return list sorted by size."""
        self.logger.info("cmd get files and sizes launched")
        c = self.connection.open_connection()
        while True:
            try:
                files = self.data_in.get(block=False, timeout=0.3)
            except Empty:
                time.sleep(0.2)
                continue
            else:
                if files[0] is None:
                    self.logger.info("cmd files and sizes finished")
                    return

                file_paths = ' '.join(files)
                cmd = 'for i in {} ; do find $i ; done'.format(file_paths)
                sin, sout, serr = c.exec_command(cmd)
                out = sout.readlines()

                all_files = ' '.join(out)
                cmd = 'du {}'.format(all_files)
                sin, sout, serr = c.exec_command(cmd)
                sizes_files = sout.readlines()

                self.logger.debug("Remote command get files and sizes has finished")

                data = {}
                for size_file in sizes_files:
                    listed = re.split("\t", size_file)
                    size = int(listed[0])
                    file = str(listed[1]).rstrip("\n")
                    data[file] = size

                sorted_by_size = dict(sorted(data.items(), key=lambda x: x[1]))
                self.logger.info("Remote command get files and sizes putting on queue")
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
        self.pipeline_out = Queue()
        self.private_key = str
        self.reload = reload

        self.config_parser = config_parser(self.config)

        self.hosts = self.config_parser.get_hosts()

    def get_local_files_and_sizes(self):
        pass

    def initialize(self):
        self.logger.debug("Initializing host %s", self.host)

        config = self.config_parser.get_connection_config(self.host)
        files = self.config_parser.get_files(self.host)
        host = self.config_parser.get_address(self.host)

        host_connection_pool = HostConnectionPool(config)
        connection_pool = host_connection_pool.initialize_pool()

        pipeline_queue_in = Queue()
        pipeline = Pipeline(pipeline_queue_in, self.pipeline_out, host, connection_pool, self.reload)
        command_pipeline = pipeline.command_pipeline
        pipeline_process = Process(target=command_pipeline)
        pipeline_process.start()

        pipeline_queue_in.put(files)

        while True:
            self.logger.debug("pipeline subprocess is alive")
            if not pipeline_process.is_alive():
                self.logger.debug("pipeline subprocess has finished")
                return
            time.sleep(0.2)

    def run(self):
        # TODO convert self.hosts to input from config manager
        reload_event = Event()
        result = []

        # First stage, for each host resolve files and get their size.
        for self.host in self.hosts:
            # pipeline = Init(host, reload_event, self.config_parser, self.pipeline_out)
            pipeline = self.initialize
            pipeline_process = Process(target=pipeline)
            pipeline_process.start()

        # Second stage. Main loop
        res = ""
        while True:
            self.logger.debug("main loop")

            try:
                res = self.pipeline_out.get(block=False, timeout=0.3)
            except Empty:
                if not pipeline_process.is_alive():
                    self.logger.debug("Pipeline finished")
                    break
                time.sleep(0.3)
            else:
                result.append(res)
                time.sleep(0.3)

            if self.reload.is_set():
                self.logger.info("Scheduler reloading")
                reload_event.set()
                pipeline_process.join()

                return

        # Last try to get from queue
        while True:
            try:
                res = self.pipeline_out.get(timeout=0.3)
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
        files = self.data_in.get()
        filenames_ok = 0

        files_and_sizes_queue_in = Queue()
        files_and_sizes_queue_out = Queue()
        remote_command = RemoteCommands(connection=self.connections[0], data_in=files_and_sizes_queue_in, data_out=files_and_sizes_queue_out)
        files_and_sizes_sorted = remote_command.get_files_and_sizes
        # local_files_and_sizes = remote_command.get_local_files_and_sizes
        remote_files_processing = Process(target=files_and_sizes_sorted)
        remote_files_processing.start()

        checksum_queue_in = Queue()
        checksum_queue_out = Queue()
        checksum_command = RemoteCommands(connection=self.connections[0], data_in=checksum_queue_in, data_out=checksum_queue_out)
        remote_checksum = checksum_command.calculate_file_hash
        remote_checksum_calculation_process = Process(target=remote_checksum)
        remote_checksum_calculation_process.start()

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


        # Parse outputs, create inputs, control data flow between processes.
        files_and_sizes_queue_in.put(files)
        download_list = []
        while True:
            self.logger.debug("Pipeline loop")
            if self.reload.is_set():
                self.logger.info("RELOAD - Pipeline terminating command processes")
                checksum_queue_in.put([None])
                download_queue_in.put([None])
                local_check_queue_in.put([None])

                return

            # Files and sizes
            self.logger.debug("Check files and sizes")
            try:
                f = files_and_sizes_queue_out.get(block=False, timeout=0.3)
            except Empty:
                pass
            else:
                self.logger.debug("Create download list")
                download_list = []
                for filename, _ in f.items():
                    download_list.append(filename)
                self.logger.debug("putting on checksum queue")
                checksum_queue_in.put(download_list)

            # Remote checksum subprocess
            self.logger.debug("Check checksum queue")
            try:
                file = checksum_queue_out.get(block=False, timeout=0.2)
            except Empty:
                pass
            else:
                print('{} - {}'.format("CHECKSUM", file))
                for k, v in file.items():
                    filename = k
                    hash = v
                self.data_out.put({filename: hash})
                self.logger.debug("putting on dload queue")
                download_queue_in.put([filename])

            # Download subprocess
            self.logger.debug("Checking dload queue")
            try:
                dloaded_file = download_queue_out.get(block=False, timeout=0.2)
            except Empty:
                pass
            else:
                self.logger.debug("Putting on check queue")
                local_check_queue_in.put([dloaded_file])

            # Local check subprocess
            self.logger.debug("Checking finall status")
            try:
                dload_status = local_check_queue_out.get(block=False, timeout=0.2)
            except Empty:
                pass
            else:
                # TODO Implement checks and retries
                self.logger.debug("File downloaded")
                for filename, ok in dload_status.items():
                    if not ok:
                        self.logger.debug("File is corrupted %s", filename)
                        download_queue_in.put(filename)
                    else:
                        self.logger.debug("File is ok")
                        filenames_ok += 1
                        self.data_out.put({self.host_id: filename})

                # When work done kill all subprocesses
                if int(len(download_list)) == int(filenames_ok):
                    # Poison pill
                    self.logger.info("Terminating pipeline subprocesses")
                    files_and_sizes_queue_in.put([None])
                    checksum_queue_in.put([None])
                    download_queue_in.put([None])
                    local_check_queue_in.put([None])

                    local_check_command_process.join()
                    self.logger.debug("local check command subprocess finished")

                    get_file_process.join()
                    self.logger.debug("get file subprocess finished")

                    remote_checksum_calculation_process.join()
                    self.logger.debug("checksum calculation subprocess finished")

                    remote_files_processing.join()
                    self.logger.debug("remote file processing subprocess finished")

                    self.logger.info("Pipeline finished")

                    break
            time.sleep(0.2)

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

    reload_scheduler = Event()
    scheduler = Scheduler(scheduler_configuration=scheduler_config, configuration=config, config_parser=ConfigParser, reload=reload_scheduler)
    schedule = scheduler.run
    process = Process(target=schedule)
    process.start()
    # reload_scheduler.set()
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
