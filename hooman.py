import boto3
import logging
import os
import shutil
import sys

from botocore.config import Config
from time import sleep
from logging.handlers import TimedRotatingFileHandler

AWS_CONFIG = Config(
    retries=dict(
        max_attempts=100
    )
)

# optional schedule class in schedule.py to add logic when to send files
# default is to send all pictures to AWS
try:
    from schedule import check_schedule
except ImportError:
    class Schedule():
        def check_schedule(self, image_file_name):
            # optional logic
            return True


def make_dir_if_not_exists(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)


def detect_hooman(image_file):
    client = boto3.client('rekognition', config=AWS_CONFIG)
    result = False
    try:
        with open(image_file, 'rb') as image:
            response = client.detect_labels(Image={'Bytes': image.read()}, MaxLabels=10, MinConfidence=90)
        for label in response['Labels']:
            if label['Name'] == 'Human' or label['Name'] == 'People' or label['Name'] == 'Person':
                logging.info(label['Name'] + ' detected with: ' + str(label['Confidence']) + ' confidence')
                result = True
                break
    except PermissionError as e:
        # might get permissions error as we might try to open file that is currently being uploaded
        # if this happens we just skip this file
        logging.error(e.strerror)
    return result


def main():
    import argparse
    parser = argparse.ArgumentParser(description='')
    parser.add_argument("-i", "--images", required=True, help="path to image directory")
    parser.add_argument("-p", "--processed_images", required=True, help="path to processed image directory")
    parser.add_argument('--log-level', dest='log_level', default='info', help='logging level',
                        choices=['debug', 'info', 'warning', 'error', 'critical'])

    args = parser.parse_args()

    if sys.platform == 'win32':
        logpath = os.getenv('APPDATA')
    else:
        logpath = os.getenv('HOME')

    logging.basicConfig(format='%(asctime)s %(levelname)s [%(pathname)s,%(lineno)d]:%(message)s',
                        level=getattr(logging, args.log_level.upper()),
                        handlers=[TimedRotatingFileHandler(os.path.join(logpath, 'hooman.log'),
                                                           when='midnight'),
                                  logging.StreamHandler()])

    make_dir_if_not_exists(args.processed_images)
    hoomans = os.path.join(args.processed_images, "hoomans")
    make_dir_if_not_exists(hoomans)
    nothing = os.path.join(args.processed_images, "nothing")
    make_dir_if_not_exists(nothing)

    schedule = Schedule()

    while True:
        image_files = [os.path.join(directory, f) for directory, subdir, flist in os.walk(args.images) for f in flist if
                       "jpg" in f]

        for image_file in image_files:
            image_file_name = os.path.basename(image_file)
            if schedule.check_schedule(image_file_name):
                if detect_hooman(image_file):
                    shutil.move(image_file, os.path.join(hoomans, image_file_name))
                else:
                    shutil.move(image_file, os.path.join(nothing, image_file_name))
            else:
                os.remove(image_file)
        sleep(30)


if __name__ == '__main__':
    main()
