"""
Running the Xetra ETL application
"""
from distutils.command import config
import logging
import logging.config

import yaml


def main():
    """
    Entrypoint to run Xetra ETL job
    """

    # Parsing YAML file
    config_path = 'configs/xetra_report1_config.yml'
    config = yaml.safe_load(open(config_path))
    #configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    logger.info("This is a test")



if __name__ == "__main__":
    main()