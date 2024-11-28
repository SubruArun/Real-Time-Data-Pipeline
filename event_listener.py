from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
import logging
import os
from pathlib import Path
from queue import Queue
from threading import Thread

from tenacity import retry, stop_after_attempt, wait_fixed
from data_pre_processing import data_pre_processing


class FileHandler(FileSystemEventHandler):
    def __init__(self, file_queue):
        self.file_queue = file_queue  # queue to hold file path

    def on_created(self, event):
        if event.src_path.endswith('.csv'):
            try:
                dataset_path = os.path.normpath(event.src_path)
                print(f"New file found & added to queue - {dataset_path}")
                self.file_queue.put(dataset_path)
            except Exception as e:
                logging.error(f"Error processing file {dataset_path}: {e}")

def process_files(file_queue):
    while True:
        dataset_path = file_queue.get()
        if dataset_path:
            try:
                data_pre_processing(dataset_path=dataset_path)
            except Exception as e:
                logging.error(f"Error processing file {dataset_path}: {e}")
            file_queue.task_done()

@retry(stop=stop_after_attempt(10), wait=wait_fixed(2))
def start_observer():
    file_queue = Queue()  # queue to hold files
    observer = Observer()
    observer.schedule(FileHandler(file_queue), path='.\data', recursive=False)

    # # test retry
    # raise Exception("error to trigger retry")

    # file processing thread
    processing_thread = Thread(target=process_files, args=(file_queue,))
    processing_thread.daemon = True  # ensure processing thread stops when the main process stops
    processing_thread.start()

    # observing directory
    observer.start()
    logging.info("Observer started...")

    # keeping alive
    while True:
        time.sleep(1)


if __name__ == "__main__":
    try:
        start_observer()
    except KeyboardInterrupt:
        logging.error(f"Manually Interrupted | Observer is Stopping | Wait for few seconds")
    except Exception as e:
        logging.error(f"Observer failed after multiple retries: {e}")
