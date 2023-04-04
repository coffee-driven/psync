import logging
import time
import re

from abc import ABC, abstractmethod
import hashlib
from multiprocessing import Queue
from queue import Empty


def logger():
    logger = logging.getLogger()
    return logger


class LocalCommands:
    def __init__(self, data_in: Queue, data_out: Queue):
        self.data_in = data_in
        self.data_out = data_out
        self.logger = logger()

    def get_local_checksum(self):
        self.logger.info("cmd check local file launched")
        files = []
        while True:
            try:
                files = self.data_in.get(block=True, timeout=0.3)
            except Empty:
                continue
            else:
                if files[0] is None:
                    self.logger.info("cmd check local file finished")
                    return

                for file in files:
                    data = []
                    hasher = hashlib.md5()

                    remote_path, local_store = file
                    local_path = "{}{}".format(local_store, remote_path)

                    with open(local_path, 'rb') as f:
                        data = f.readlines()

                    hasher.update(data[0])
                    checksum = hasher.hexdigest()

                    self.logger.info("cmd get local checksum putting on queue %s", file)
                    self.data_out.put({remote_path: {"local_path": local_path, "checksum": checksum},},)

    def get_local_files(self):
        pass


class Strategy(ABC):

    @abstractmethod
    def command(path: str):
        pass


class FindFile(Strategy):
    def command(path):
        cmd = '''find {} -type f -exec du {{}} \\;'''.format(path)
        return cmd


class FindDir(Strategy):
    def command(self, path):
        cmd = '''find {} -type d -exec du {{}} \\;'''.format(path)
        return cmd


class GetFileType(Strategy):
    def command(self, path):
        cmd = '''file = {}
                if [ -d ${{file}} ] ; then
                    printf '%s\n' "directory"
                 elif [ -f ${{file}} ] ; then
                    printf '%s\n' "file"
                 else
                    printf '%s\n' "missing"
                 fi
              '''.format(path)
        return cmd


class FileCommandsContext:
    def __init__(self, strategy: Strategy, connection, path: str):
        self.connection = connection
        self._strategy = strategy
        self.path = path

    @property
    def strategy(self) -> Strategy:
        return self._strategy

    @strategy.setter
    def strategy(self, strategy: Strategy) -> None:
        self._strategy = strategy

    def execute(self):
        try:
            cmd = self.connection.run(self.strategy.command(self.path))
        except Exception as e:
            return (e, [])
        else:
            out = cmd.stdout
            result = out.splitlines()
            return ("", result)


class RemoteCommands:
    def __init__(self, connection, data_in: Queue, data_out: Queue):
        self.connection = connection
        self.data_in = data_in
        self.data_out = data_out
        self.logger = logger()

    def calculate_file_hash(self) -> dict:
        """Calculate hash of file"""
        self.logger.info("cmd calculate file hash launched")
        files = []
        c = self.connection.open_connection()

        while True:
            try:
                files = self.data_in.get(block=False)
            except Empty:
                continue
            if files[0] is None:
                self.logger.info("cmd calculate file hash been terminated")
                return

            self.logger.debug("received item from queue")

            # Batch of files 
            # file_paths = ' '.join(files)
            # shell_cmd = 'md5sum {}'.format(file_paths)

            for file in files:
                shell_cmd = 'md5sum {}'.format(file)
                cmd = c.run(shell_cmd)
                cmd_out = cmd.stdout

                # md5sum splits the data by two spaces (Debian10)
                checksum, _filename = re.split("  ", cmd_out)
                filename = _filename.rstrip('\n')

                self.logger.debug("Checksum gained for %s", filename)

                self.logger.info("cmd calculate file putting on queue")
                self.data_out.put({filename: checksum})

    def get_file(self):
        self.logger.info("cmd get file launched")
        c = self.connection.open_connection()

        files = ()
        while True:
            try:
                files = self.data_in.get(block=False)
            except Empty:
                continue

            if files[0] is None:
                self.logger.info("cmd get file finished")
                return

            for file in files:
                remote_path, local_path = file
                local_store = "{}/{}".format(local_path, remote_path)
                c.get(remote_path, local_store, False)
                self.logger.info("cmd get file downloading file %s to %s", remote_path, local_path)
                self.data_out.put({remote_path: True})

    def remote_sleep(self, sleep_time: int):
        self.logger.info("cmd remote sleep launched")
        c = self.connection.open_connection()

        shell_cmd = "sleep {} && echo Up!".format(sleep_time)
        cmd = c.run(shell_cmd)
        cmd_out = cmd.stdout

        self.logger.info("cmd remote command putting on queue")
        self.data_queue.put([cmd_out])

    @staticmethod
    def parse_files_and_sizes(sizes_and_files: list) -> dict:
        """
            This method takes care of parsing the output from remote commands. It's separated because there might be multiple variations
            of output format eq. space separated, tab separated, etc..
        """
        res = {}

        separator = '\t'  # this might be converted to function that identify the separator
        for record in sizes_and_files:
            size_file = record.split(separator)
            res[size_file[1]] = int(size_file[0])

        return res

    def get_files_and_sizes(self) -> dict:
        """Find files, get their size return list sorted by size."""
        self.logger.info("cmd get files and sizes launched")
        connection = self.connection.open_connection()

        while True:
            try:
                files = self.data_in.get(block=False)
            except Empty:
                time.sleep(0.2)
                continue
            else:
                if files[0] is None:
                    self.logger.info("cmd files and sizes finished")
                    return

                found_files = []
                present_files = []
                data = {
                    "found": {},
                    "not_found": {},
                }

                for file_path in files:
                    found_files = []
                    remote_command = FileCommandsContext(GetFileType(), connection, file_path)
       
                    err, file_type = remote_command.execute()
                    if err:
                        self.logger.error("Get file type error %s", err)
                        continue

                    if file_type[0] == "missing":
                        data["not_found"][file_path] = -1
                        continue

                    if file_type[0] == "directory":
                        present_files.append(file_path)
                        remote_command.strategy = FindDir()
                        err, found_files = remote_command.execute()
                        if err:
                            self.logger.error("Find dir error: %s", err)
                            continue

                    if file_type[0] == "file":
                        present_files.append(file_path)
                        remote_command.strategy(FindFile)
                        err, found_files = remote_command.execute()
                        if err:
                            self.logger.error("Find file error: %s", err)
                            continue

                    files_sizes_dict = self.parse_files_and_sizes(found_files)
                    sorted_by_size = dict(sorted(files_sizes_dict.items(), key=lambda x: x[1]))
                    data["found"].update(sorted_by_size)

                self.logger.debug("Remote command get files and sizes has finished")
                self.logger.info("Remote command get files and sizes putting on queue")

                self.data_out.put(data)
