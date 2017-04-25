#!/usr/bin/env python2
# -*- Coding: utf-8 -*-

import logging
import urllib2
import urlparse
import base64
import fnmatch
import os
import os.path
import shutil
import re
import xml.dom.minidom as dom
import mmap
from cStringIO import StringIO


def add_basic_auth(request, credentials):
    if credentials is not None:
        authheader = base64.encodestring(
            '%s:%s' % credentials).replace('\n', '')
        request.add_header("Authorization", "Basic %s" % authheader)


def webdav_glob(baseurl, credentials=None, pattern='*'):
    baseurl = baseurl + '/' if baseurl[-1] <> '/' else baseurl
    serverurl = urlparse.urlparse(baseurl)
    serverurl = serverurl.scheme + '://' + serverurl.netloc

    request = urllib2.Request(
        baseurl, data='<D:propfind xmlns:D="DAV:"><D:prop><D:displayname/></D:prop></D:propfind>')
    request.get_method = lambda: 'PROPFIND'
    logging.debug('PROPFIND ' + baseurl)
    add_basic_auth(request, credentials)
    request.add_header('Depth', '1')
    request.add_header('Brief', 't')
    xmldata = ''.join([line for line in urllib2.urlopen(request)])
    xmldata = re.sub('<(/?)[^> ]+:', '<\\1', xmldata)  # remove namespaces
    for href in dom.parse(StringIO(xmldata)).getElementsByTagName('href'):
        path = href.firstChild.nodeValue
        url = serverurl + \
            path if path[0] == '/' else urlparse.urljoin(baseurl, path)
        if url <> baseurl:
            if url[-1] == '/':
                # collection
                for child in webdav_glob(url, credentials, pattern):
                    yield child
            elif not '!svn' in url:
                url = urlparse.urlparse(url)
                if fnmatch.fnmatch(url.path, pattern):
                    yield url


def url_download_file(url, destfile, credentials=None):
    if not os.path.isdir(os.path.dirname(destfile)):
        os.makedirs(os.path.dirname(destfile))
    logging.debug('GET ' + url.geturl())
    request = urllib2.Request(url.geturl())
    add_basic_auth(request, credentials)
    try:
        if os.path.isfile(destfile + '.part'):
            os.remove(destfile + '.part')
            logging.debug('removed ' + destfile + '.part')
        with file(destfile + '.part', 'wb') as f:
            logging.debug('writing ...')
            for data in urllib2.urlopen(request):
                f.write(data)
                f.flush()
            logging.debug('... done')
        shutil.move(destfile + '.part', destfile)
        logging.debug('renamed ' + destfile + '.part to ' + destfile)
    except Exception:
        logging.exception('while downloading file')
        if os.path.isfile(destfile + '.part'):
            os.remove(destfile + '.part')
            logging.debug('removed ' + destfile + '.part')
        raise


def compute_md5(filename):
    import hashlib
    md5 = hashlib.md5()
    with file(filename, 'rb') as f:
        while True:
            data = f.read(4096)
            if not data:
                break
            md5.update(data)
    return md5.hexdigest()


class TrackDb:

    def __init__(self, basedir):
        import sqlite3
        self.basedir = re.sub('(/|\\\\)$', '', os.path.abspath(basedir))
        self.conn = sqlite3.connect(os.path.join(basedir, '.download_history'))
        c = self.conn.cursor()
        c.execute(
            "select count(*) from sqlite_master where type='table' and name='files'")
        if c.fetchone()[0] == 0:
            c.execute(
                "create table files(path text not null, md5 text, dl_date datetime not null)")
            self.conn.commit()

    def is_in_db(self, filename):
        filename = os.path.abspath(filename)
        filename = filename[len(self.basedir):].replace('\\', '/')
        c = self.conn.cursor()
        c.execute('select count(*) from files where path=?', [filename])
        return c.fetchone()[0] > 0

    def add_in_db(self, filename, md5):
        md5 = compute_md5(filename) if md5 is None else md5
        filename = os.path.abspath(filename)
        filename = filename[len(self.basedir):].replace('\\', '/')
        c = self.conn.cursor()
        c.execute("insert into files(path, md5, dl_date) values (?, ?, datetime('now'))", [
                  filename, md5])
        self.conn.commit()
        logging.debug('[tracking db] added ' + filename)

    def close(self):
        self.conn.close()


def url_download_dir(baseurl, credentials=None, targetdir='.', flatten=False, download_if_exists=True, download_if_existed=False, addmd5=False):
    if not os.path.isdir(targetdir):
        os.makedirs(targetdir)

    trackDb = None
    if not download_if_existed:
        download_if_exists = False
        trackDb = TrackDb(targetdir)
        logging.debug('[tracking db] using tracking db ' +
                      os.path.join(targetdir, '.download_history'))
    try:
        baseurl = baseurl + '/' if baseurl[-1] <> '/' else baseurl
        for url in webdav_glob(baseurl, credentials):
            targetfile = os.path.abspath(os.path.join(
                targetdir, url.geturl()[len(baseurl):]).replace('/', os.sep))
            if flatten:
                targetfile = os.path.abspath(os.path.join(
                    targetdir, os.path.basename(targetfile)))
            if os.path.basename(targetfile)[0] <> '.':
                logging.debug(targetfile)
                do = download_if_exists or not os.path.isfile(targetfile)
                if not do:
                    logging.debug(
                        '\tfile exists locally and download_if_exists is False, skipping download')
                if do and not download_if_existed:
                    do = do and not trackDb.is_in_db(targetfile)
                    if not do:
                        logging.debug(
                            '\t[tracking db] file already exists in db, skipping download')

                if do:
                    url_download_file(url, targetfile, credentials)
                    md5 = None
                    if addmd5 or trackDb and not targetfile.endswith('.md5'):
                        md5 = compute_md5(targetfile)
                    if addmd5:
                        with file(targetfile + '.md5', 'w') as md5file:
                            md5file.write(md5 + '\n')
                        logging.debug(
                            '\tMD5 sum : ' + md5 + ', saved to ' + os.path.basename(targetfile + '.md5'))
                    if trackDb:
                        trackDb.add_in_db(targetfile, md5)
                        logging.debug('\t[tracking db] added to db')
    finally:
        if trackDb:
            logging.debug('[tracking db] closed db')
            trackDb.close()


def rglob(path, pattern='*'):
    for root, dirs, files in os.walk(path):
        for filename in fnmatch.filter(files, pattern):
            yield os.path.abspath(os.path.join(root, filename))


def exists(url, credentials=None):
    head = urllib2.Request(url)
    head.get_method = lambda: 'HEAD'
    logging.debug('HEAD ' + url)
    add_basic_auth(head, credentials)
    try:
        urllib2.urlopen(head)
        return True
    except urllib2.URLError:
        return False


def makedirs(url, credentials):
    current = re.sub('[^/]*$', '', url.geturl())
    dirs = []
    while not exists(current):
        if urlparse.urlparse(current).path == '':
            raise Exception(current + ' Does not exist !')
        dirs.insert(0, current)
        current = re.sub('[^/]*/?$', '', current)

    for d in dirs:
        mkcol = urllib2.Request(d)
        mkcol.get_method = lambda: 'MKCOL'
        add_basic_auth(mkcol, credentials)
        urllib2.urlopen(mkcol)


def url_upload_file(filename, url, credentials=None):

    makedirs(url, credentials)

    # put file to temporary destination
    opener = urllib2.build_opener(urllib2.HTTPHandler)
    with file(filename, 'rb') as uploadfile:
        mmapped = mmap.mmap(uploadfile.fileno(), 0, access=mmap.ACCESS_READ)
        put = urllib2.Request(
            url.geturl() + '.part', data=mmapped)
        put.get_method = lambda: 'PUT'
        add_basic_auth(put, credentials)
        put.add_header('Content-Type', 'application/octet-stream')
        logging.debug('PUT ' + url.geturl() + '.part')
        opener.open(put)
        mmapped.close()

    # move .part file to target destination
    move = urllib2.Request(url.geturl() + '.part')
    move.get_method = lambda: 'MOVE'
    move.add_header('Destination', url.geturl())
    move.add_header('Overwrite', 'T')
    logging.debug('MOVE ' + url.geturl() + '.part ' + url.path)
    add_basic_auth(move, credentials)
    urllib2.urlopen(move)


def url_upload_dir(localdir, baseurl, credentials=None, upload_if_exists=False):
    baseurl = baseurl + '/' if baseurl[-1] <> '/' else baseurl
    localdir = os.path.abspath(localdir)
    for filename in rglob(localdir):
        if os.path.basename(filename)[0] <> '.':
            path = filename[1 + len(localdir):].replace(os.sep, '/')
            targeturl = baseurl + path
            if upload_if_exists or not exists(targeturl, credentials):
                url_upload_file(filename, urlparse.urlparse(targeturl), credentials)

from abc import ABCMeta, abstractmethod


class Task:
    __metaclass__ = ABCMeta

    @abstractmethod
    def run(self):
        pass


class WebdavDownloadTask(Task):

    def __init__(self, remoteurl, localdir, credentials=None, flatten=False, download_if_exists=True, download_if_existed=False, addmd5=False):
        self.remoteurl = remoteurl
        self.localdir = localdir
        self.credentials = credentials
        self.flatten = flatten
        self.download_if_exists = download_if_exists
        self.download_if_existed = download_if_existed
        self.addmd5 = addmd5

    def run(self):
        url_download_dir(
            self.remoteurl,
            credentials=self.credentials,
            targetdir=self.localdir,
            flatten=self.flatten,
            download_if_exists=self.download_if_exists,
            download_if_existed=self.download_if_existed,
            addmd5=self.addmd5
        )


class WebdavUploadTask(Task):

    def __init__(self, localdir, targeturl, credentials=None, upload_if_exists=False):
        self.localdir = localdir
        self.targeturl = targeturl
        self.credentials = credentials
        self.upload_if_exists = upload_if_exists

    def run(self):
        url_upload_dir(
            self.localdir,
            self.targeturl,
            self.credentials,
            self.upload_if_exists
        )

__all__ = ['WebdavDownloadTask', 'WebdavUploadTask']

if __name__ == '__main__':
    import sys
    import time
    import optparse

    parser = optparse.OptionParser(
        usage="usage: %prog [options]\nSynchonizes a local directory with a remote WebDAV folder")

    parser.add_option("-a", "--action", dest="action", default='DOWNLOAD',
                      help="action to perform (UPLOAD or DOWNLOAD)")

    parser.add_option("-u", "--url", dest="url",
                      help="WebDAV folder URL", metavar="URL")

    parser.add_option("-d", "--dir", dest="dir",
                      help="local directory", metavar="DIR")

    parser.add_option("-U", "--username", dest="username",
                      help="Basic auth Username", metavar="USERNAME")

    parser.add_option("-P", "--password", dest="password",
                      help="Basic auth Password", metavar="PASSWORD")

    parser.add_option("-f", "--flatten", dest="flatten", action="store_true", default=False,
                      help="put all files at the same level in target directory")

    parser.add_option("-l", "--loop", dest="loop",  action="store_true", default=False,
                      help="loop forever")

    parser.add_option(
        "-t", "--interval", dest="interval",  type="int", default=60,
                      help="interval in seconds between loops (meaningless if the -l option is inactive)")

    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", default=False,
                      help="be verbose about what is being done")

    dwngroup = optparse.OptionGroup(
        parser, 'DOWNLOAD Options', 'Options available only in Download mode')

    dwngroup.add_option(
        "-e", "--downloadifexists", dest="downloadifexists", action="store_true", default=False,
                              help=" download file even if it already exists locally")

    dwngroup.add_option(
        "-E", "--downloadifexisted", dest="downloadifexisted",  action="store_true", default=False,
                              help="download file even if it has been downloaded once (if this option is inactive, a file name .download_db will be created in target directory and will keep track of previously downloaded files)")

    dwngroup.add_option('-m', "--add-md5", dest="addmd5", action="store_true", default=False,
                        help="for each file, generate a <file>.md5 that contains the md5 checksum")

    parser.add_option_group(dwngroup)

    uploadgroup = optparse.OptionGroup(
        parser, 'UPLOAD Options', 'Options available only in Upload mode')

    uploadgroup.add_option(
        "-r", "--uploadifexists", dest="uploadifexists",  action="store_true", default=False,
                           help="upload file even if a file with the same name exists remotely")

    parser.add_option_group(uploadgroup)

    (options, args) = parser.parse_args()

    if not options.action in ['UPLOAD', 'DOWNLOAD']:
        sys.stderr.write(
            "the --action parameter must eval to 'UPLOAD' or 'DOWNLOAD'\n")
        sys.exit(-1)

    if options.url is None:
        sys.stderr.write(
            'the --url parameter is required (' + sys.argv[0] + ' -h for help)\n')
        sys.exit(-1)

    if options.dir is None:
        sys.stderr.write(
            'the --dir parameter is required (' + sys.argv[0] + ' -h for help)\n')
        sys.exit(-1)

    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=(
        logging.DEBUG if options.verbose else logging.INFO))

    while True:
        task = None
        creds = None
        if not options.username is None:
            creds = (options.username, options.password)
        if options.action == 'DOWNLOAD':
            task = WebdavDownloadTask(
                options.url, options.dir, credentials=creds, flatten=options.flatten,
                                      download_if_exists=options.downloadifexists, download_if_existed=options.downloadifexisted, addmd5=options.addmd5)
        else:
            task = WebdavUploadTask(
                localdir=options.dir, targeturl=options.url, credentials=creds, upload_if_exists=options.uploadifexists)
        try:
            task.run()
        except Exception, e:
            logging.exception(options.action + ' FAILED')
            if not options.loop:
                sys.exit(-2)

        if not options.loop:
            break

        logging.debug('sleeping for ' + str(options.interval) + ' seconds ...')
        time.sleep(options.interval)
