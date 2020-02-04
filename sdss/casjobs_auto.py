import time
from datetime import datetime

import requests

from SciServer.Authentication import login
import SciServer.CasJobs as cj
import SciServer.SciDrive as sd

from constants import (TABLE_DOWNLOAD_URL,
                       HEADERS,
                       REQUEST_DATA,
                       CASJOBS_CONTAINER)


class CasJobsError(Exception):
    pass


class CasjobsDownloader:
    """
    A Simple Wrapper Class used to download any query as a dataFrame and
    upload it to sciserver-files (if provided)
    """

    def __init__(self, uname=None, passwd=None, _login=True):
        if _login:
            self.token = login(uname, passwd)
        self.uname = uname
        self.passwd = passwd

    def download_query(self, query, table_name, save_to='data', context='MyDB'):
        """Perform a query based on the context given"""
        if context != 'MyDB':
            jobid = cj.submitJob(query, context=context)
            cj.waitForJob(jobid, verbose=True)
            job_status = cj.getJobStatus(jobid)
            if job_status['Success'] != 5:
                raise CasJobsError('Error Performing the query, {}'.format(job_status))
        # Now the results are safely saved in myDB
        # You can go ahead and download by using a session
        self.download_csv(table_name)
        self._download_from_scidrive(table_name, save_to)

    def download_csv(self, table_name):
        """Download the save the CSV to scidrive and download it."""
        self._create_container()
        casjobs_session = requests.Session()
        res = casjobs_session.post('https://apps.sciserver.org/login-portal/Account/Login',
                                   data={
                                       'callbackUrl': 'http://skyserver.sdss.org/CasJobs/login.aspx?nexturl=MyDB.aspx',
                                       'username': self.uname,
                                       'password': self.passwd
                                   })
        assert res.status_code == 200
        context_cookie = requests.cookies.create_cookie(domain='skyserver.sdss.org', name='SELECTED_CONTEXT', value='MyDB')
        # casjobs_session.cookies.update(context_cookie)
        REQUEST_DATA['customQuery'] = 'SELECT * FROM {0}'.format(table_name)
        REQUEST_DATA['scidrivePath'] = '/casjobs_container'
        HEADERS['Referer'] = TABLE_DOWNLOAD_URL.format(table_name, 'TABLE', 'MyDB', 'normal')
        res = casjobs_session.post(TABLE_DOWNLOAD_URL.format(table_name, 'TABLE', 'MyDB', 'normal'),
                                   data=REQUEST_DATA,
                                   headers=HEADERS,
                                   allow_redirects=True)

    def _create_container(self):
        """Clear the scidrive container"""
        print('Creating a new container path')
        try:
            sd.createContainer(CASJOBS_CONTAINER)
        except:
            pass
        time.sleep(3)

    def _download_from_scidrive(self, table_name, save_to, max_tries=10, verbose=True):
        """Check if the table is ready to download and download it once ready"""
        tgt_file_path = CASJOBS_CONTAINER + '/' + table_name + '_{}'.format(self.uname) + '.csv'
        count = 0
        while True:
            list_of_files = sd.directoryList(CASJOBS_CONTAINER)['contents']
            for file in list_of_files:
                if file['path'].strip() == tgt_file_path:
                    sd.download(tgt_file_path, localFilePath='{0}/{1}.csv'.format(save_to, table_name))
                    is_deleted = sd.delete(tgt_file_path)
                    print('Successfully downloaded the table csv, deleted in SciDrive: ', is_deleted)
                    return
            count += 1
            if count > max_tries:
                break
            if verbose:
                print('Waiting')
            time.sleep(10)
        print('could not download file')


if __name__ == '__main__':
    import os
    cjd = CasjobsDownloader(os.environ['SCISERVER_USERNAME'], os.environ['SCISERVER_PASSWORD'])
    cjd.download_query(query='SELECT * FROM SDSS_DR12', table_name='SDSS_DR12', context='MyDB')
