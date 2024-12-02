import logging

def log_info(log_type, log_message):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
    )

    if log_type == "info":
        logging.info(log_message)
    elif log_type == "error":
        logging.error(log_message)
    elif log_type == "warning":
        logging.warning(log_message)
    elif log_type == "debug":
        logging.debug(log_message)
