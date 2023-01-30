#!/usr/bin/env python3

import argparse
import os
import time

from fabric import Connection
from paramiko import client

from multiprocessing import Process, Queue


SENDER_SCRIPT_PATH = "/tmp/sender.py"


class HostConnectionPool:
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
        sout = self.connection.exec_command("echo cool")
        print(sout)
        q.put([sin, sout, serr])
        print("Woken up!")


class RemoteSynchronization:
    def connect(self, config: dict):
        print("Creating connection")
        self.host = config['host']
        self.port = int(config['port']) or 22
        self.username = config['username']
        self.private_key = config['private_key']
        self.local_storage = config['default_local_storage']

        if not os.path.exists(self.private_key):
            print("Private key is inaccessible")
            exit(2)

        connection = Connection(host=self.host, user=self.username, port=self.port, connect_kwargs={"key_filename": self.private_key})

        print("Opening connection")
        try:
            connection.open()
        except Exception as e:
            print('{}: {}'.format("Couldn't connect via ssh", e))
            exit(2)

        self.connection = connection

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

        self.connection.run("")

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

    con_list = [HostConnectionPool(config) for _ in range(1, 4, 1)]
    for c in con_list:
        try:
            c.open_connection()
        except Exception as e:
            print("Connection doesn't work. Removing from list")
            print(e)
            con_list.remove(c)

    q = Queue()
    host_con = con_list[0]
    p = Process(target=host_con.sleep, args=(q,))
    p.start()

    print(q.get())

    exit()
    connection_list = []
    for i in range(1, 4, 1):
        connector = HostConnectionPool(config)
        try:
            con = connector.open_connection()
        except Exception as e:
            print("Connection unsuccessful")
            print(e)
        else:
            connection_list.append(con)

    [print(id(x)) for x in connection_list]
    

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
