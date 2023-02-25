import json
import logging
import time

from multiprocessing import Process, Queue
from queue import Empty

from psync.config_parser import ConfigParser
from psync.conn import HostConnectionPool
from psync.pipeline import Pipeline


def logger():
    logger = logging.getLogger()

    return logger


class Scheduler:
    """
        Take files, syncing option and schedule synchronization subprocesses.
    """
    def __init__(self, config_parser, result_queue: Queue) -> None:
        self.connection_pool = {}
        self.remote_files = {}
        self.local_files = {}
        self.hosts = []
        self.logger = logger()
        self.pipeline_out = Queue()
        self.private_key = str
        self.config_parser = config_parser
        self.hosts = self.config_parser.get_hosts()
        self.result_queue = result_queue

    def initialize(self, host):
        self.logger.debug("Initializing host %s", host)

        config = self.config_parser.get_connection_config(host)
        files = self.config_parser.get_files(host)

        host_connection_pool = HostConnectionPool(config)
        connection_pool = host_connection_pool.initialize_pool()

        pipeline_queue_in = Queue()
        pipeline = Pipeline(pipeline_queue_in, self.pipeline_out, host, connection_pool, self.config_parser)
        command_pipeline = pipeline.command_pipeline
        pipeline_process = Process(target=command_pipeline)
        pipeline_process.start()

        pipeline_queue_in.put(files)

        while pipeline_process.is_alive():
            self.logger.debug("Waiting for pipeline")
            time.sleep(0.5)
        return

    def run(self):
        pipelines = []

        # Launch command pipeline for each host
        for host in self.hosts:
            init_pipeline = self.initialize
            init_pipeline_process = Process(target=init_pipeline, args=(host,))
            init_pipeline_process.start()
            pipelines.append(init_pipeline_process)

        # Process pipelines output
        while True:
            self.logger.info("Checking pipelines output")

            if not pipelines and self.pipeline_out.empty():
                break

            try:
                pipeline_output = self.pipeline_out.get(block=True, timeout=0.3)
            except Empty:
                self.logger.info("Pipeline queue is empty")

                for pipeline in pipelines:
                    if not pipeline.is_alive():
                        self.logger.info("Pipeline finished. %s", pipeline)
                        pipelines.remove(pipeline)

                time.sleep(0.5)
            else:
                self.result_queue.put(pipeline_output)
                time.sleep(0.5)

                continue

        self.logger.info("All pipelines finished")

        return 0
