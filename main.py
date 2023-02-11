#!/usr/bin/env python3

import argparse
import json
import logging
import os
import re
import time

from difflib import Differ
from fabric import Connection
from paramiko import client

from multiprocessing import Process, Queue, Event
from queue import Empty


def logger():
    logging.basicConfig(filename="/tmp/test.log", filemode='a')
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

    def get_local_files(self):
        pass



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

                self.logger.info("cmd calculate file putting on queue")
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

                found_files = []
                not_found_files = []
                data = {
                    "found": [],
                    "not_found": [],
                }

                file_paths = ' '.join(files)
                self.logger.debug("FILES %s", file_paths)
                cmd = 'for i in {} ; do find $i ; done'.format(file_paths)
                sin, sout, serr = c.exec_command(cmd)
                sout_lines = sout.readlines()

                if not sout_lines:
                    self.logger.warning("Find didn't find any files")
                    data["not_found"] = files
                    self.data_out.put(data)
                    return

                found_files = [x.strip('\n') for x in sout_lines]

                diff = Differ()
                diff_res = diff.compare(sorted(found_files), sorted(files))
                for line in diff_res:
                    if line[0] == "+":
                        not_found_file = '-1\t{}'.format(line[2:])
                        not_found_files.append(not_found_file)

                file_full_paths = ' '.join(found_files)
                cmd = 'du {}'.format(file_full_paths)
                sin, sout, serr = c.exec_command(cmd)
                sizes_files = sout.readlines()

                [sizes_files.append(nf) for nf in not_found_files]

                self.logger.debug("Remote command get files and sizes has finished")

                for size_file in sizes_files:
                    listed = re.split("\t", size_file)
                    size = int(listed[0])
                    file = str(listed[1]).rstrip("\n")
                    if int(size) < 0:
                        data["not_found"].append((file, size))
                    else:
                        data["found"].append((file, size))

                sorted_by_size = dict(sorted(data.items(), key=lambda x: x[1]))
                self.logger.info("Remote command get files and sizes putting on queue")
                self.data_out.put(sorted_by_size)


# TODO convert self.hosts to input from config manager
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

    def initialize(self, host):
        self.logger.debug("Initializing host %s", host)

        config = self.config_parser.get_connection_config(host)
        files = self.config_parser.get_files(host)
        host = self.config_parser.get_address(host)

        host_connection_pool = HostConnectionPool(config)
        connection_pool = host_connection_pool.initialize_pool()

        pipeline_queue_in = Queue()
        pipeline = Pipeline(pipeline_queue_in, self.pipeline_out, host, connection_pool, self.reload)
        command_pipeline = pipeline.command_pipeline
        pipeline_process = Process(target=command_pipeline)
        pipeline_process.start()

        pipeline_queue_in.put(files)

        while pipeline_process.is_alive():
            self.logger.debug("Waiting for pipeline")
            time.sleep(0.5)
        return

    def run(self):
        reload_event = Event()
        result = []
        pipelines = []

        # Launch command pipeline for each host
        for host in self.hosts:
            finish = Event()
            init_pipeline = self.initialize
            init_pipeline_process = Process(target=init_pipeline, args=(host,))
            init_pipeline_process.start()
            pipelines.append(init_pipeline_process)
        
        # Process pipelines output
        out = ""
        while pipelines:
            self.logger.info("main")

            try:
                pipeline_output = self.pipeline_out.get(block=True, timeout=0.3)
            except Empty:
                self.logger.debug("Pipeline queue is empty")
                
                for pipeline in pipelines:
                    if not pipeline.is_alive():
                        self.logger.info("Pipeline finished. %s", pipeline)
                        pipelines.remove(pipeline)

                time.sleep(0.5)
            else:
                result.append(pipeline_output)
                time.sleep(0.5)

            if self.reload.is_set():
                self.logger.info("Scheduler reloading")
                reload_event.set()
                pipeline_process.join()

                return

        self.logger.info("All pipelines finished")

        # Last try to get from queue
        while True:
            try:
                res = self.pipeline_out.get(timeout=0.3)
            except Empty:
                break
            else:
                result.append(res)

        result_dict = {}
        for i, r in enumerate(result):
            result_dict[i] = r

        result_json = json.dumps(result_dict, indent = 1)
        print(result_json)

        return

class Pipeline:
    def __init__(self, data_in: Queue, data_out: Queue, host_id: str, connections: HostConnectionPool, reload: Event) -> None:
        Process.__init__(self)
        self.connections = connections
        self.data_in = data_in
        self.data_out = data_out
        self.host_id = host_id
        self.logger = logger()
        self.reload = reload

        # Final status output
        self.status = {
            "host": self.host_id,
            "files": {},
        }


    def command_pipeline(self):
        self.logger.info("Pipeline launched")

        local_files_queue_in = Queue()
        local_files_queue_out = Queue()
        get_local_files = LocalCommands(data_in=local_files_queue_in, data_out=local_files_queue_out)
        get_local_files_command = get_local_files.get_local_files
        get_local_files_process = Process(target=get_local_files_command)
        get_local_files_process.start()

        files_and_sizes_queue_in = Queue()
        files_and_sizes_queue_out = Queue()
        get_remote_files_sizes = RemoteCommands(connection=self.connections[0], data_in=files_and_sizes_queue_in, data_out=files_and_sizes_queue_out)
        get_remote_files_sizes_command = get_remote_files_sizes.get_files_and_sizes
        get_remote_files_sizes_process = Process(target=get_remote_files_sizes_command)
        get_remote_files_sizes_process.start()
        self.logger.debug("files and sizes launched")

        checksum_queue_in = Queue()
        checksum_queue_out = Queue()
        get_remote_files_checksum = RemoteCommands(connection=self.connections[0], data_in=checksum_queue_in, data_out=checksum_queue_out)
        get_remote_files_checksum_command = get_remote_files_checksum.calculate_file_hash
        get_remote_files_checksum_process = Process(target=get_remote_files_checksum_command)
        get_remote_files_checksum_process.start()
        self.logger.debug("remote checksum launched")

        download_queue_in = Queue()
        download_queue_out = Queue()
        get_remote_file = RemoteCommands(connection=self.connections[1], data_in=download_queue_in, data_out=download_queue_out)
        get_remote_file_command = get_remote_file.get_file
        get_remote_file_process = Process(target=get_remote_file_command)
        get_remote_file_process.start()
        self.logger.debug("downloading launched")


        local_check_queue_in = Queue()
        local_check_queue_out = Queue()
        check_dloaded_file_checksum = LocalCommands(data_in=local_check_queue_in, data_out=local_check_queue_out)
        check_dloaded_file_checksum_command = check_dloaded_file_checksum.check_local_file
        check_dloaded_file_checksum_process = Process(target=check_dloaded_file_checksum_command)
        check_dloaded_file_checksum_process.start()
        self.logger.debug("local check of downloaded files launched")

        files = self.data_in.get()
        self.logger.debug("%s - pipeline received files", self.host_id)
        files_and_sizes_queue_in.put(files)
        files_ok = 0

        queues = [files_and_sizes_queue_in,
                  checksum_queue_in,
                  download_queue_in,
                  local_check_queue_in,
                 ]

        commands = [get_remote_files_sizes_process,
                    get_remote_files_checksum_process,
                    get_remote_file_process,
                    check_dloaded_file_checksum_process,
                   ]

        while True:
            self.logger.debug("Pipeline loop")

            if self.reload.is_set():
                self.logger.info("RELOAD - Pipeline terminating command processes")
                _ = [q.put([None])for q in queues]
                return

            # Stage files and sizes
            self.logger.debug("Check files and sizes")
            try:
                remote_files_sizes = files_and_sizes_queue_out.get(block=True, timeout=0.2)
            except Empty:
                pass
            else:
                if not remote_files_sizes["found"]:
                    self.logger.warning("Found no files to backup")
                    _ = [q.put([None])for q in queues]
                    _ = [q.close() for q in queues]
                    _ = [q.join_thread() for q in queues]
                    _ = [c.terminate() for c in commands]
                    self.logger.info("%s - Pipeline finished", self.host_id)
                    return

                self.logger.debug("Creating download list")
                download_list = []
                for file_size in remote_files_sizes["found"]:
                    download_list.append(file_size[0])
                    self.status["files"][file_size[0]] = {}
                    self.status["files"][file_size[0]]["size"] = file_size[1]

                    self.logger.debug("putting on checksum queue %s", file_size[0])
                    checksum_queue_in.put(download_list)
                
                for file_size in remote_files_sizes["not_found"]:
                    self.logger.error("File not found %s", file_size[0])

            # Stage remote checksum subprocess
            self.logger.debug("Checking checksum queue")
            try:
                file = checksum_queue_out.get(block=True, timeout=0.1)
            except Empty:
                pass
            else:
                self.logger.debug("Processing output from checksum queue")
                for k, v in file.items():
                    filename = k
                    hash = v

                self.status["files"][filename]["hash"] = hash
                
                self.logger.debug("putting on dload queue")
                download_queue_in.put([filename])

            # TODO Filter files that are present and actual, download only missing files


            # Stage download subprocess
            self.logger.debug("Checking dload queue")
            try:
                dloaded_file = download_queue_out.get(block=True, timeout=0.2)
            except Empty:
                pass
            else:
                self.logger.debug("Putting on check queue")
                local_check_queue_in.put([dloaded_file])

            # Stage local check subprocess
            self.logger.debug("Checking final status")
            try:
                dload_status = local_check_queue_out.get(block=True, timeout=0.2)
            except Empty:
                pass
            else:
                # TODO Implement checks and retries
                self.logger.debug("File downloaded")
                for filename, ok in dload_status.items():
                    if not ok:
                        self.logger.debug("File is corrupted %s", filename)
                        download_queue_in.put(filename)
                        self.status["files"][file_size[0]]["state"] = "corrupted"
                    else:
                        self.logger.debug("File is ok")
                        files_ok += 1
                        self.status["files"][file_size[0]]["state"] = "synced"

                # Finish: kill all subprocesses
                if int(len(download_list)) == int(files_ok):
                    self.logger.info("Terminating pipeline subprocesses")
                    # Poison pill
                    _ = [q.put([None])for q in queues]
                    self.logger.info("poisoned")

                    get_local_files_process.join()
                    self.logger.debug("get local files subprocess finished")

                    get_remote_files_sizes_process.join()
                    self.logger.debug("get remote files and sizes subprocess finished")

                    get_remote_files_checksum_process.join()
                    self.logger.debug("get remote file checksum subprocess finished")

                    get_remote_file_process.join()
                    self.logger.debug("get remote file subprocess finished")

                    self.logger.info("Pipeline putting on queue")
                    self.data_out.put(self.status)

                    _ = [q.close() for q in queues]
                    _ = [q.join_thread() for q in queues]
                    self.logger.debug("All queues closed")
                    self.logger.info("%s - Pipeline has finished", self.host_id)

                    return

                time.sleep(0.2)

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
                "/home/myfile": {
                    "sync_options": "options",
                    "local_path": "local_path",
                },
            },
        },
        "vm2": {
            "host": "127.0.0.2",
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
