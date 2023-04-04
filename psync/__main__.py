import argparse
import json
import logging
import time


from multiprocessing import Process, Queue
from psync.config_parser import ConfigParser
from psync.scheduler import Scheduler
from queue import Empty


def setup_logger():
    logging.basicConfig(filename="/tmp/test.log", filemode='a')
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    return 0


def main():

    setup_logger()

    with open(args.config, "r") as jsonfile:
        config = json.load(jsonfile)

    configurator = ConfigParser(config)
    configurator.validate()

    final_result_queue = Queue()
    scheduler = Scheduler(config_parser=configurator, result_queue=final_result_queue)
    cmd = scheduler.run
    process = Process(target=cmd)
    process.run()

    sync_status = []
    while True:
        if not process.is_alive() and final_result_queue.empty():
            break

        try:
            res = final_result_queue.get(block=False)
        except Empty:
            time.sleep(0.5)
        else:
            sync_status.append(res)
            time.sleep(0.3)

            result_dict = {}
            for i, r in enumerate(sync_status):
                result_dict[i] = r

            result_json = json.dumps(result_dict, indent=1)

            print(result_json)
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
                    prog = 'ProgramName',
                    description = 'Agent-less file synchronization simultaneously from multiple hosts via SSH and shell commands',
                    epilog = 'Text at the bottom of help')
    parser.add_argument('--config', dest='config', action='store', help='Configuration', required=True)

    args = parser.parse_args()

    main()
