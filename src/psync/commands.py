import logging
import time
import re

from difflib import Differ
from multiprocessing import Process, Queue
from queue import Empty


def logger():
    logging.basicConfig(filename="/tmp/test.log", filemode='a')
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.ERROR)

    return logger


class LocalCommands:
    def __init__(self, data_in: Queue, data_out: Queue):
        self.data_in = data_in
        self.data_out = data_out
        self.logger = logger()

    def get_local_checksum(self):

        import hashlib

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
                    self.data_out.put({remote_path: { "local_path": local_path, "checksum": checksum},},)

    def get_local_files(self):
        pass


class RemoteCommands:
    """
       Remote command object.
       Data queue is used for control data - sizes, hashes
       Management queue is used for siginalization
    """
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
                files = self.data_in.get(block=False, timeout=0.5)
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
                files = self.data_in.get(block=False, timeout=0.2)
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
        self.data_queue.put([out])

    @staticmethod
    def parse_files_and_sizes(sizes_and_files: list) -> dict:
        """
            This method takes care of parsing the output from remote commands. It's separated because there might be multiple variations
            of output format eq. space separated, tab separated, etc..
        """
        res = {}

        separator = '\t' # this might be converted to function that identify the separator
        for record in sizes_and_files:
            size_file = record.split(separator)
            res[size_file[1]] = int(size_file[0])
        
        return res

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
                data = {
                    "found": {},
                    "not_found": {},
                }

                file_paths = ' '.join(files)
                self.logger.debug("FILES %s", file_paths)
                
                shell_cmd = '''for file in {} ; do
                           if [ -f $file ] ; then
                               printf '%s\n' "$(du $file)"
                           elif [ -d $file ] ; then
                               find $file -type f -exec du {{}} \;
                           else
                               printf '%s' ""
                           fi
                          done'''.format(file_paths)

                try:
                    cmd = c.run(shell_cmd)
                except Exception as e:
                    self.logger.error("file sizes %s", e)
                else:
                    cmd_out = cmd.stdout
                    splitted_result = cmd_out.split('\n')
                    found_files = [x for x in splitted_result if x != '']

                if not found_files:
                    self.logger.warning("Find didn't find any files")
                    data["not_found"] = files
                    self.data_out.put(data)
                    return
                
                files_sizes_dict = self.parse_files_and_sizes(found_files)
                found_filenames = list(files_sizes_dict.keys())

                diff = Differ()
                diff_res = diff.compare(sorted(found_filenames), sorted(files))
                for line in diff_res:
                    if line[0] == "+":
                        name = str(line[2:])
                        data["not_found"][name] = -1

                sorted_by_size = dict(sorted(files_sizes_dict.items(), key=lambda x: x[1]))
                data["found"] = sorted_by_size

                self.logger.debug("Remote command get files and sizes has finished")
                self.logger.info("Remote command get files and sizes putting on queue")

                self.data_out.put(data)
