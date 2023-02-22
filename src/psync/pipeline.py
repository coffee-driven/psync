import logging
import time

from multiprocessing import Process, Queue, Event
from queue import Empty


from psync.commands import LocalCommands, RemoteCommands
from psync.conn import HostConnectionPool


def logger():
    logging.basicConfig(filename="/tmp/test.log", filemode='a')
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.ERROR)

    return logger


class Pipeline:
    def __init__(self, data_in: Queue, data_out: Queue, host_id: str, connections: HostConnectionPool, reload: Event, config_parser) -> None:
        Process.__init__(self)
        self.config_parser = config_parser
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
        check_dloaded_file_checksum_command = check_dloaded_file_checksum.get_local_checksum
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
                for filename, size in remote_files_sizes["found"].items():
                    download_list.append(filename)
                    self.status["files"][filename] = {}
                    self.status["files"][filename]["size"] = size

                    self.logger.debug("putting on checksum queue %s", filename)
                    checksum_queue_in.put(download_list)

                for filename, size in remote_files_sizes["not_found"].items():
                    self.status["files"][filename] = {"size": 0,
                                                      "checksum": None,
                                                      "state": "missing",
                                                      }

                    self.logger.error("File not found %s", filename)

            # Stage remote checksum subprocess
            self.logger.debug("Checking checksum queue")
            try:
                filename_checksum = checksum_queue_out.get(block=True, timeout=0.1)
            except Empty:
                pass
            else:
                self.logger.debug("Processing output from checksum queue")
                for k, v in filename_checksum.items():
                    filename = k
                    checksum = v

                    self.status["files"][filename]["checksum"] = checksum

                    storage = self.config_parser.get_files_local_storage(self.host_id, filename)
                    download_data = (filename, storage)

                    self.logger.debug("putting on dload queue")
                    download_queue_in.put([download_data])

            # TODO Filter files that are present and actual, download only missing files

            # Stage download subprocess
            self.logger.debug("Checking dload queue")
            try:
                dloaded_file = download_queue_out.get(block=True, timeout=0.2)
            except Empty:
                pass
            else:
                for k, v in dloaded_file.items():
                    local_path = self.config_parser.get_files_local_storage(self.host_id, k)
                    # local_store = "{}{}".format(local_path, k)
                    data = (k, local_path)

                    self.logger.debug("Putting on check queue")
                    local_check_queue_in.put([data])

            # Stage local check subprocess
            self.logger.debug("Checking final status")
            try:
                local_check_status = local_check_queue_out.get(block=True, timeout=0.2)
            except Empty:
                pass
            else:
                # TODO Implement checks and retries
                self.logger.debug("File downloaded")
                for filename, status in local_check_status.items():
                    if not status:
                        self.logger.debug("File is corrupted %s", filename)
                        download_queue_in.put([filename])
                        self.status["files"][filename]["state"] = "corrupted"
                    else:
                        self.logger.debug("File is ok")
                        files_ok += 1
                        self.status["files"][filename]["state"] = "synced"

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
