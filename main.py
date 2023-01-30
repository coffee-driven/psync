#!/usr/bin/env python3

import argparse
import logging
import os
import time

from fabric import Connection
from paramiko import client

from multiprocessing import Process, Queue


SENDER_SCRIPT_PATH = "/tmp/sender.py"


class HostConnection:
    """Connection object"""
    def __init__(self, config: dict) -> None:
        self.host = config['host']
        self.port = int(config['port']) or 22
        self.username = config['username']
        self.private_key = config['private_key']
        self.connection = object

    def open_connection(self) -> object:
        # self.connection = Connection(host=self.host, user=self.username, port=self.port, connect_kwargs={"key_filename": self.private_key})
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


class HostConnectionPool():
    def __init__(self, connection_config: dict, connections: int) -> None:
        self.conf_cfg = connection_config
        self.connections = connections

    def initialize_pool(self):
        logging.debug("Initializing pool")
        connection_pool = [HostConnection(self.conf_cfg) for _ in range(0, self.connections, 1)]
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


def main():
    parser = argparse.ArgumentParser(
                    prog = 'ProgramName',
                    description = 'What the program does',
                    epilog = 'Text at the bottom of help')
    parser.add_argument('--remote_path', dest='remote_path', help='Local path')
    parser.add_argument('--private_key', dest='private_key', action='store', help='Private key', required=True) 

    args = parser.parse_args()

    curr_sender_script_hash = 'ss33'

    config = {
        'host': "127.0.0.1",
        'port': 2022,
        'username': "bob",
        'private_key': args.private_key,
        'default_local_storage': "/tmp/",
    }

    files_and_options = [
        {"path": "/tmp/1",
         "sync_options": "-rsync -like -options",
         "local_path": "/tmp/",
         },

        {"path": "/tmp/2    ",
         "sync_options": "-some -options",
         }
    ]

    host_connection_pool = HostConnectionPool(connection_config=config, connections=3)
    connection_pool = host_connection_pool.initialize_pool()

    q = Queue()
    prcs = []
    for host_con in connection_pool:
        p = Process(target=host_con.sleep, args=(q,))
        p.start()
        prcs.append(p)

    print("getting from queue")
    for i in range(1, 4, 1):
        print(q.get())

    [x.join() for x in prcs]

    

    #remote_sync = RemoteSynchronization()
    #remote_sync.connect(config)
#
    #try:
    #    sender_script_hash = remote_sync.check_sender_script()
    #except FileNotFoundError:
    #    remote_sync.sync_sender_script()
    #else:
    #    if sender_script_hash != curr_sender_script_hash:
    #        remote_sync.sync_sender_script()
    #finally:
    #    print("Remote sender script: OK")
#
    #remote_sync.synchronize(files_and_options)
#

main()
