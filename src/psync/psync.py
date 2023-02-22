#!/usr/bin/env python3

import argparse
import json
import logging
import time

from multiprocessing import Process, Queue, Event
from queue import Empty

from psync.config_parser import ConfigParser
from psync.conn import HostConnectionPool
from psync.pipeline import Pipeline


def logger():
    logging.basicConfig(filename="/tmp/test.log", filemode='a')
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.ERROR)

    return logger


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
        # host = self.config_parser.get_address(host)

        host_connection_pool = HostConnectionPool(config)
        connection_pool = host_connection_pool.initialize_pool()

        pipeline_queue_in = Queue()
        pipeline = Pipeline(pipeline_queue_in, self.pipeline_out, host, connection_pool, self.reload, self.config_parser)
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


def main():

    scheduler_config = {
        'max_parallel_connections': 100,
    }

    with open(args.config, "r") as jsonfile:
        config = json.load(jsonfile)

    reload_scheduler = Event()
    scheduler = Scheduler(scheduler_configuration=scheduler_config, configuration=config, config_parser=ConfigParser, reload=reload_scheduler)
    schedule = scheduler.run
    process = Process(target=schedule)
    process.start()
    # reload_scheduler.set()
    process.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
                    prog = 'ProgramName',
                    description = 'What the program does',
                    epilog = 'Text at the bottom of help')
    parser.add_argument('--remote_path', dest='remote_path', help='Remote path')
    parser.add_argument('--config', dest='config', action='store', help='Configuration', required=True)

    args = parser.parse_args()

    main()
