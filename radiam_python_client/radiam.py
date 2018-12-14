#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Usage:
  radiam.py --help
  radiam.py --version
  radiam.py [--rootdir=<DIR>] [--index=<Idx>] [--mtime=<mt>] [--minsize=<ms>] [--backend]

Options:
  -h --help     Show this screen.
  -V --version     Show version.
  -d --rootdir=<DIR>  Directory to start crawling from.  [default: .]
  -i --index=<Idx>   Elasticsearch index name [default: config['index']]
  -m --mtime=<mt>  Minimum days ago for modified time (default: 0) [default: 0]
  -s --minsize=<ms>  Minimum file size in Bytes (default: 1 Bytes) [default: 1]
  -b --backend  Run the crawler on backend, monitoring the file system
"""

from docopt import docopt
from scandir import scandir
import os
import sys
# import tika
from tika import parser as tikaParser
import persistqueue
from persistqueue import Queue
try:
    import configparser as ConfigParser
except ImportError:
    import ConfigParser
import logging
import time
from datetime import datetime
import platform
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from appdirs import AppDirs

dirs = AppDirs("radiam-agent", "Compute Canada")
os.makedirs(dirs.user_data_dir, exist_ok=True)

try:
    import pwd
    import grp
except:
    pass

version = 'standalone 0.01'
__version__ = version

# cache uid/gid names
uids = []
gids = []
owners = {}
groups = {}


class FileSystemMonitor(FileSystemEventHandler):
    def __init__(self, q_dir, index_name, arguments):
        self.q_dir = q_dir
        self.index_name = index_name
        self.arguments = arguments
        self.c_set = set()
        self.d_set = set()

    def on_deleted(self, event):
        super(FileSystemMonitor, self).on_deleted(event)

        what = 'directory' if event.is_directory else 'file'
        if what == 'file' and not file_excluded(event.src_path):
            self.d_set.add(event.src_path)
        while len(self.d_set)!=0:
            try_connection_in_worker_del(index_name, self.d_set.pop())
            logger.info("Deleted %s: %s", what, event.src_path)

    def on_created(self, event):
        super(FileSystemMonitor, self).on_created(event)

        what = 'directory' if event.is_directory else 'file'
        if what == 'file' and not file_excluded(event.src_path):
            self.c_set.add(event.src_path)
        while len(self.c_set)!=0:
            metadata = get_file_meta(self.c_set.pop(), arguments)
            if metadata is not None:
                try_connection_in_worker(index_name, metadata)
                logging.info("Created or Modified %s: %s", what, event.src_path)

    # def on_modified(self, event):
    #     super(FileSystemMonitor, self).on_modified(event)
    #
    #     what = 'directory' if event.is_directory else 'file'
    #     # logging.info("Modified %s: %s", what, event.src_path)
    #
    #     if what == 'file' and not file_excluded(event.src_path):
    #         self.c_set.add(event.src_path)
    #     while len(self.c_set)!=0:
    #         metadata = get_file_meta(self.c_set.pop(), arguments)
    #         print ('modify metadata')
    #         print (metadata)
    #         if metadata is not None:
    #         #     pass
    #         # else:
    #             try_connection_in_worker(index_name, metadata)
    #             logging.warning("Modified %s: %s", what, event.src_path)

    def on_moved(self, event):
        super(FileSystemMonitor, self).on_moved(event)

        what = 'directory' if event.is_directory else 'file'
        if what == 'file' and not file_excluded(event.src_path) and not file_excluded(event.dest_path):
            self.d_set.add(event.src_path)
            self.c_set.add(event.dest_path)
        while len(self.c_set)!=0:
            metadata = get_file_meta(self.c_set.pop(), arguments)
            if metadata is not None:
                try_connection_in_worker(index_name, metadata)
        while len(self.d_set)!=0:
            try_connection_in_worker_del(index_name, self.d_set.pop())
            logging.info("Moved %s: from %s to %s", what, event.src_path,
                         event.dest_path)


def try_connection_in_worker_del(index_name,path):
    while True:
        try:
            filename = os.path.basename(path)
            parentdir = os.path.abspath(os.path.join(path, os.pardir))
            res = es.search(index='radiam-index', body={"query": {
                "constant_score": {
                    "filter": {
                        "bool": {
                            "must": [
                                {"term": {"filename": filename}},
                                {"term": {"path_parent": parentdir}}
                            ]
                        }
                    }
                }
            }
            })
            for doc in res['hits']['hits']:
                es.delete(index=index_name, doc_type='file', id=doc["_id"])
            return
        except exceptions.ConnectionError:
            time.sleep(10)
            pass

def try_connection(index_name, q_dir, base_dir, arguments):
    while True:
        try:
            index_create(index_name)
            # start at the base directory
            q_dir.put(base_dir)
            files = []
            worker(q_dir, files, index_name, arguments)
            return
        except exceptions.ConnectionError:
            time.sleep(10)
            pass

def try_connection_in_worker(index_name, metadata):
    while True:
        try:
            es.index(index=index_name, doc_type='file', body=metadata)
            return
        except exceptions.ConnectionError:
            time.sleep(10)
            pass

def load_config():
    """This is the load config function.
    It checks for config file and loads in
    the config settings.
    """
    configsettings = {}
    config = ConfigParser.ConfigParser()
    dir_path = os.path.dirname(os.path.realpath(__file__))
    # check if env var for config file and use that
    try:
        configfile = os.environ['RADIAM_CONFIG']
    except KeyError:
        configfile = '%s/radiam.cfg' % dir_path
        pass
    # Check for config file
    if not os.path.isfile(configfile):
        print('Config file %s not found', configfile)
        sys.exit(1)
    config.read(configfile)
    try:
        d = config.get('excludes', 'dirs')
        dirs = d.split(',')
        configsettings['excluded_dirs'] = set(dirs)
    except ConfigParser.NoOptionError:
        configsettings['excluded_dirs'] = set([])
    try:
        f = config.get('excludes', 'files')
        files = f.split(',')
        configsettings['excluded_files'] = set(files)
    except ConfigParser.NoOptionError:
        configsettings['excluded_files'] = set([])
    try:
        d = config.get('includes', 'dirs')
        dirs = d.split(',')
        configsettings['included_dirs'] = set(dirs)
    except ConfigParser.NoOptionError:
        configsettings['included_dirs'] = set([])
    try:
        f = config.get('includes', 'files')
        files = f.split(',')
        configsettings['included_files'] = set(files)
    except ConfigParser.NoOptionError:
        configsettings['included_files'] = set([])
    try:
        configsettings['es_host'] = config.get('elasticsearch', 'host')
    except ConfigParser.NoOptionError:
        configsettings['es_host'] = "localhost"
    try:
        configsettings['es_port'] = int(config.get('elasticsearch', 'port'))
    except ConfigParser.NoOptionError:
        configsettings['es_port'] = 9200
    try:
        configsettings['es_user'] = config.get('elasticsearch', 'user')
    except ConfigParser.NoOptionError:
        configsettings['es_user'] = ""
    try:
        configsettings['es_password'] = config.get('elasticsearch', 'password')
    except ConfigParser.NoOptionError:
        configsettings['es_password'] = ""
    try:
        configsettings['index'] = config.get('elasticsearch', 'indexname')
    except ConfigParser.NoOptionError:
        configsettings['index'] = ""
    try:
        configsettings['es_timeout'] = int(config.get('elasticsearch', 'timeout'))
    except ConfigParser.NoOptionError:
        configsettings['es_timeout'] = 10
    try:
        configsettings['es_maxsize'] = int(config.get('elasticsearch', 'maxsize'))
    except ConfigParser.NoOptionError:
        configsettings['es_maxsize'] = 10
    try:
        configsettings['es_max_retries'] = int(config.get('elasticsearch', 'maxretries'))
    except ConfigParser.NoOptionError:
        configsettings['es_max_retries'] = 0
    try:
        configsettings['es_wait_status_yellow'] = config.get('elasticsearch', 'wait')
    except ConfigParser.NoOptionError:
        configsettings['es_wait_status_yellow'] = "False"
    try:
        configsettings['es_chunksize'] = int(config.get('elasticsearch', 'chunksize'))
    except ConfigParser.NoOptionError:
        configsettings['es_chunksize'] = 500
    try:
        configsettings['index_shards'] = int(config.get('elasticsearch', 'shards'))
    except ConfigParser.NoOptionError:
        configsettings['index_shards'] = 5
    try:
        configsettings['index_replicas'] = int(config.get('elasticsearch', 'replicas'))
    except ConfigParser.NoOptionError:
        configsettings['index_replicas'] = 1
    try:
        configsettings['index_refresh'] = config.get('elasticsearch', 'indexrefresh')
    except ConfigParser.NoOptionError:
        configsettings['index_refresh'] = "1s"
    try:
        configsettings['disable_replicas'] = config.get('elasticsearch', 'disablereplicas')
    except ConfigParser.NoOptionError:
        configsettings['disable_replicas'] = "False"
    try:
        configsettings['index_translog_size'] = config.get('elasticsearch', 'translogsize')
    except ConfigParser.NoOptionError:
        configsettings['index_translog_size'] = "512mb"
    try:
        configsettings['es_scrollsize'] = int(config.get('elasticsearch', 'scrollsize'))
    except ConfigParser.NoOptionError:
        configsettings['es_scrollsize'] = 100
    return configsettings, configfile

def log_setup():
    """This is the log set up function.
    It configures log output for diskover.
    """
    radiam_logger = logging.getLogger('radiam')
    radiam_logger.setLevel(logging.INFO)
    es_logger = logging.getLogger('elasticsearch')
    es_logger.setLevel(logging.WARNING)
    urllib3_logger = logging.getLogger('urllib3')
    urllib3_logger.setLevel(logging.WARNING)
    requests_logger = logging.getLogger('requests')
    requests_logger.setLevel(logging.WARNING)

    logging.addLevelName(
        logging.INFO, "\033[1;32m%s\033[1;0m"
                      % logging.getLevelName(logging.INFO))
    logging.addLevelName(
        logging.WARNING, "\033[1;31m%s\033[1;0m"
                         % logging.getLevelName(logging.WARNING))
    logging.addLevelName(
        logging.ERROR, "\033[1;41m%s\033[1;0m"
                       % logging.getLevelName(logging.ERROR))
    logging.addLevelName(
        logging.DEBUG, "\033[1;33m%s\033[1;0m"
                       % logging.getLevelName(logging.DEBUG))
    logformatter = '%(asctime)s [%(levelname)s][%(name)s] %(message)s'

    loglevel = logging.INFO
    logging.basicConfig(format=logformatter, level=loglevel)

    return radiam_logger
# some file types are not supported:
# 422 (Unprocessable Entity) status,
# 415(Unsupported Media Type) status
# It may be solved with setting paths to exclude and file types to exclude in the future
def find_metadata(dir, content=False):
    parsed = tikaParser.from_file(dir, 'http://lib-garnett.lib.sfu.ca:9998/')
    # return parsed["status"]
    if parsed["status"] == 200:
        if content:
            return parsed["content"]
        else:
            return parsed["metadata"]
    else:
        return parsed

def index_create(indexname):
    """This is the es index create function.
    It checks for existing index and deletes if
    there is one with same name. It also creates
    the new index and sets up mappings.
    """
    logger.info('Checking es index: %s', indexname)
    # check for existing es index
    if es.indices.exists(index=indexname):
        # delete existing index
        logger.warning('es index exists, deleting')
        es.indices.delete(index=indexname, ignore=[400, 404])
    # set up es index mappings and create new index
    mappings = {
            "settings": {
                "index" : {
                    "number_of_shards": config['index_shards'],
                    "number_of_replicas": config['index_replicas']
                }
            },
            "mappings": {
                # "directory": {
                #     "properties": {
                #         "filename": {
                #             "type": "keyword"
                #         },
                #         "path_parent": {
                #             "type": "keyword"
                #         },
                #         "filesize": {
                #             "type": "long"
                #         },
                #         "items": {
                #             "type": "long"
                #         },
                #         "items_files": {
                #             "type": "long"
                #         },
                #         "items_subdirs": {
                #             "type": "long"
                #         },
                #         "owner": {
                #             "type": "keyword"
                #         },
                #         "group": {
                #             "type": "keyword"
                #         },
                #         "last_modified": {
                #             "type": "date"
                #         },
                #         "last_access": {
                #             "type": "date"
                #         },
                #         "last_change": {
                #             "type": "date"
                #         },
                #         "hardlinks": {
                #             "type": "integer"
                #         },
                #         "inode": {
                #             "type": "long"
                #         },
                #         "tag": {
                #             "type": "keyword"
                #         },
                #         "tag_custom": {
                #             "type": "keyword"
                #         },
                #         "crawl_time": {
                #             "type": "float"
                #         },
                #         "change_percent_filesize": {
                #             "type": "float"
                #         },
                #         "change_percent_items": {
                #             "type": "float"
                #         },
                #         "change_percent_items_files": {
                #             "type": "float"
                #         },
                #         "change_percent_items_subdirs": {
                #             "type": "float"
                #         },
                #         "worker_name": {
                #             "type": "keyword"
                #         },
                #         "indexing_date": {
                #             "type": "date"
                #         }
                #     }
                # },
                "file": {
                    "properties": {
                        "filename": {
                            "type": "keyword"
                        },
                        "extension": {
                            "type": "keyword"
                        },
                        "path_parent": {
                            "type": "keyword"
                        },
                        "filesize": {
                            "type": "long"
                        },
                        "owner": {
                            "type": "keyword"
                        },
                        "group": {
                            "type": "keyword"
                        },
                        "last_modified": {
                            "type": "date"
                        },
                        "last_access": {
                            "type": "date"
                        },
                        "last_change": {
                            "type": "date"
                        },
                        "tika_metadata": {
                            "type": "text"
                        },
                        "indexed_by": {
                            "type": "keyword"
                        },
                        "indexing_date": {
                            "type": "date"
                        }
                    }
                }
            }
        }

    logger.info('Creating es index')
    es.indices.create(index=indexname, body=mappings)
    time.sleep(.5)

def get_file_meta(path, arguments):
    """This is the get file meta data function.
    It scrapes file meta and ignores files smaller
    than minsize Bytes, newer than mtime
    and in excluded_files. Returns file meta dict.
    """

    try:
        if file_excluded(path):
            return None

        mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime = os.lstat(path)

        # Skip files smaller than minsize cli flag
        if size < arguments['--minsize']:
            return None

        # Convert time in days (mtime cli arg) to seconds
        time_sec = arguments['--mtime'] * 86400
        file_mtime_sec = time.time() - mtime
        # Only process files modified at least x days ago
        if file_mtime_sec < time_sec:
            return None

        # convert times to utc for es
        mtime_utc = datetime.utcfromtimestamp(mtime).isoformat()
        atime_utc = datetime.utcfromtimestamp(atime).isoformat()
        ctime_utc = datetime.utcfromtimestamp(ctime).isoformat()

        # try to get owner user name
        # first check cache
        if uid in uids:
            owner = owners[uid]
        # not in cache
        else:
            owner = uid
            if not platform.system() == 'Windows':
                try:
                    owner = pwd.getpwuid(uid).pw_name.split('\\')
                    # remove domain before owner
                    if len(owner) == 2:
                        owner = owner[1]
                    else:
                        owner = owner[0]
                # if we can't find the owner's user name, use the uid number
                except KeyError:
                    owner = uid
            # store it in cache
            if not uid in uids:
                uids.append(uid)
                owners[uid] = owner

        # try to get group name
        # first check cache
        if gid in gids:
            group = groups[gid]
        # not in cache
        else:
            group = gid
            if not platform.system() == 'Windows':
                try:
                    group = grp.getgrgid(gid).gr_name.split('\\')
                    # remove domain before group
                    if len(group) == 2:
                        group = group[1]
                    else:
                        group = group[0]
                # if we can't find the group name, use the gid number
                except KeyError:
                    group = gid
            # store in cache
            if not gid in gids:
                gids.append(gid)
                groups[gid] = group

        # get time
        indextime_utc = datetime.utcnow().isoformat()

        # get absolute path of parent directory
        parentdir = os.path.abspath(os.path.join(path, os.pardir))

        # create file metadata dictionary
        filemeta_dict = {
            "filename": os.path.basename(path),
            "extension": os.path.splitext(os.path.basename(path))[1][1:].strip().lower(),
            "path_parent": parentdir,
            "filesize": size,
            "owner": owner,
            "group": group,
            "last_modified": mtime_utc,
            "last_access": atime_utc,
            "last_change": ctime_utc,
            "tika_metadata": str(find_metadata(path)),
            "indexed_by": owner,
            "indexing_date": indextime_utc,
            # "_type": "file"
        }
    except (IOError, OSError) as e:
        return False

    except FileNotFoundError as e:
        return False

    return filemeta_dict

def file_excluded(filepath):
    """Return True if path or ext in excluded_files set,
    False if not in the set"""
    filename = os.path.basename(filepath)
    extension = os.path.splitext(filename)[1][1:].strip().lower()
    # return if filename in included list (whitelist)
    if filename in config['included_files']:
        return False
    # check for extension in and . (dot) files in excluded_files
    if (not extension and 'NULLEXT' in config['excluded_files']) or \
                            '*.' + extension in config['excluded_files'] or \
            (filename.startswith('.') and u'.*' in config['excluded_files']) or \
            (filename.endswith('~') and u'*~' in config['excluded_files']) or \
            (filename.startswith('~$') and u'~$*' in config['excluded_files']):
        return True
    # check for filename in excluded_files set
    if filename in config['excluded_files']:
        return True
    return False

"""
worker function processes the queue of directories. At the current dir, if the entry is a dir, put
the dir into the queue of directories and if the entry is a file,find the metadata and push them 
onto es. Finally all the items in the queue will be processed.
"""
def worker(q_dir, files, index_name, arguments):
    while True:
        try:
            path = q_dir.get_nowait()
            try:
                for entry in scandir(path):
                    if entry.is_dir(follow_symlinks=False):
                        q_dir.put(os.path.join(path, entry.name))
                    elif entry.is_file(follow_symlinks=False):
                        metadata = get_file_meta(os.path.join(path, entry.name), arguments)
                        # add it to radiam index
                        if metadata is None:
                            pass
                        else:
                            # this should be a subprocess eventually
                            try_connection_in_worker(index_name, metadata)
                            # es.index(index=index_name, doc_type='file', body=metadata)
                            files.append(entry.name)
            except (PermissionError, OSError) as e:
                logger.warning(e)
                pass
            q_dir.task_done()
        except persistqueue.exceptions.Empty:
            break

# load config file into config dictionary
config, configfile = load_config()

import connections
# create Elasticsearch connection
connections.connect_to_elasticsearch()
from connections import es_conn as es
from connections import exceptions

if __name__ == "__main__":
    arguments = docopt(__doc__, version=__version__)
    logger = log_setup()
    queue_on_disk = os.path.join(dirs.user_data_dir, "radiam_queue")
    base_dir = arguments['--rootdir']
    index_name = eval(arguments['--index']) \
        if arguments['--index'] == "config['index']" \
        else arguments['--index']
    arguments['--mtime'] = int(arguments['--mtime'])
    arguments['--minsize'] = int(arguments['--minsize'])
    run_backend = arguments['--backend']
    q_dir = Queue(queue_on_disk)
    if run_backend :
        event_handler = FileSystemMonitor(q_dir, index_name, arguments)
        observer = Observer()
        observer.schedule(event_handler, base_dir, recursive=True)
        observer.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()
    else:
        try_connection(index_name, q_dir, base_dir, arguments)