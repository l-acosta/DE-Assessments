import logging
import sys
from etl.load import load
from etl.extract import extract
from etl.transformation import transformation


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    if len(sys.argv) < 3:
        logger.info(
            "The ETL require 2 arguments: the input data path, and the output data path"
        )
        return

    # extract
    logger.info("Start extract step")
    input_data_path = sys.argv[1]
    logger.info(f"Input data path is {input_data_path}")
    data = extract(path=input_data_path)
    logger.info("extract step done")

    # transform
    logger.info("Start transform step")
    data_transform = transformation(df=data)
    logger.info("transform step done")

    # load
    logger.info("Start load step")
    output_data_path = sys.argv[2]
    logger.info(f"Output data path is {output_data_path}")
    load(df=data_transform, path_to_write=output_data_path)
    logger.info("end step done")


if __name__ == "__main__":
    main()
