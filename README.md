webdavsync
==========

Synchonizes a local directory with a remote WebDAV folder

Usage :
-------

```
Usage: webdavsync.py [options]
Synchonizes a local directory with a remote WebDAV folder

Options:
  -h, --help            show this help message and exit
  -a ACTION, --action=ACTION
                        action to perform (UPLOAD or DOWNLOAD)
  -u URL, --url=URL     WebDAV folder URL
  -d DIR, --dir=DIR     local directory
  -U USERNAME, --username=USERNAME
                        Basic auth Username
  -P PASSWORD, --password=PASSWORD
                        Basic auth Password
  -f, --flatten         put all files at the same level in target directory
  -l, --loop            loop forever
  -t INTERVAL, --interval=INTERVAL
                        interval in seconds between loops (meaningless if the
                        -l option is inactive)
  -v, --verbose         be verbose about what is being done

  DOWNLOAD Options:
    Options available only in Download mode

    -e, --downloadifexists
                         download file even if it already exists locally
    -E, --downloadifexisted
                        download file if it has been downloaded once (if this
                        option is inactive, a file named .download_db will be
                        created in target directory and will keep track of
                        previously downloaded files)
    -m, --add-md5       for each file, generate a <file>.md5 that contains the md5
                        checksum

  UPLOAD Options:
    Options available only in Upload mode

    -r, --uploadifexists
                        upload file even if a file with the same name exists
                        remotely

```
