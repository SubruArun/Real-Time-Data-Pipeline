import polling2
import os
import logging
from tenacity import retry, stop_after_attempt, wait_fixed

from data_processing import data_pre_processing


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def check_for_new_files(folder_path, known_files):
    current_files = set(os.listdir(folder_path))
    new_files = current_files - known_files
    return new_files

@retry(stop=stop_after_attempt(10), wait=wait_fixed(2))
def start_observer():
    folder_path = './data/'

    if not os.path.exists(folder_path):
        logging.error(f"Folder {folder_path} does not exist | Retrying...")
        raise Exception(f"Folder {folder_path} does not exist")

    known_files = set(os.listdir(folder_path))
    logging.info(f"Found existing files : {known_files}")

    while True:
        try:
            new_files = polling2.poll(
                lambda: check_for_new_files(folder_path, known_files),
                step=5,  # Polling interval
                poll_forever=True
            )
            if new_files:
                logging.info(f"New files detected : {new_files}")
                known_files.update(new_files)
                for each_file in list(new_files):
                    data_pre_processing(dataset_path=f"./data/{each_file}")
        except Exception as e:
            logging.error(f"Exception occured : {e} | Retrying... | Existing Files : {known_files}")
            raise Exception(f"Exception occured : {e} | Retrying...")


if __name__ == "__main__":
    try:
        start_observer()
    except KeyboardInterrupt:
        logging.error(f"Manually Interrupted | Observer is Stopping | Wait for few seconds")
    except Exception as e:
        logging.error(f"Observer failed after multiple retries: {e}")
