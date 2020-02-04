"""
This script runs the preprocessing pipeline in sciserver-compute
1. Upload the code repo to sciserver-files.
2. Install Swarp
3. Mount Data Volumes
4. Randomize the csv based on dataSize
5. Operate on the data and apply swarp to the dataset
6. Save the result in sciserver-files.
"""
import logging

from SciServer.Authentication import login
import SciServer.Files as sf

from sdss.casjobs_auto import CasjobsDownloader
from utils import zip_dir

# Set logging
logger = logging.getLogger('SciServerJobSubmitter')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
sh = logging.StreamHandler()
sh.setFormatter(formatter)
logger.addHandler(sh)


class SciServerJobRunner:
    JOBS_DOMAIN = ''
    DOCKER_IMAGE = ''

    @staticmethod
    def set_job_config(domain, image_name):
        SciServerJobRunner.JOBS_DOMAIN = domain
        SciServerJobRunner.DOCKER_IMAGE = image_name
        logger.info('Set {} jobs_domain and it runs in  {} docker image'.format(domain, image_name))

    @staticmethod
    def login_sciserver(uname, passwd):
        login(uname, passwd)
        logger.info('Successfully logged in to science server')

    @staticmethod
    def upload_repo(volume):
        archive = zip_dir('code', os.path.dirname(__file__), format='tar')
        pass

    @staticmethod
    def install_swarp():
        pass

    @staticmethod
    def download_table():
        pass


if __name__ == '__main__':
    import os
    SciServerJobRunner.login_sciserver(os.environ['SCISERVER_USERNAME'], os.environ['SCISERVER_PASSWORD'])
    SciServerJobRunner.set_job_config('Small Jobs Domain', 'Python + R')
    SciServerJobRunner.upload_files(path_=os.getcwd(), volume='AstroResearch')

