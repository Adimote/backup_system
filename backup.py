#!/usr/bin/env python
import os
import shutil
import subprocess
import fnmatch
import re
import sys
import boto
from datetime import datetime

file_path = os.path.dirname(os.path.realpath(__file__))
level = 0

DATE_FORMAT = "%y-%m-%d-%H:%M"


def run_command_with_output(command):
    """
        Run a command whilst printing any errors/messages over stdout
    """
    print "Running command: '{}'".format(" ".join(command))
    p = subprocess.Popen(command,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    while p.returncode is None:
        p.poll()
        a = p.stdout.readline()
        if a:
            print a,
    p.wait()


def tar(snapshot_file, level, file_dir, date_string, files):
    """
        Run a Tar command for backing up
    """
    tar = [
        "tar",
        "--listed-incremental={}".format(snapshot_file),
        "--level={}".format(level),
        "-cvzf",
        "{}/backup_level_{}_{}.tar.gz".format(file_dir, level, date_string),
        " ".join(files)
    ]
    run_command_with_output(tar)


def untar(backup_file):
    """
        Run an untar command for undoing backups
    """

    untar = [
            "tar",
            "--listed-incremental=/dev/null",
            "-xvf",
            backup_file
        ]

    run_command_with_output(untar)


def _snapshot_name(file_dir, level):
    return "{}/snapshot_level_{}.file".format(
        file_dir,
        level
    )


def get_snapshot_if_exists(file_dir, level):
    snapshot = _snapshot_name(file_dir, level)
    if level > 0:
        prev_snapshot = _snapshot_name(
            file_dir,
            level-1
        )
        if (os.path.exists(prev_snapshot)):
            shutil.copyfile(prev_snapshot, snapshot)
        else:
            raise Exception("Previous snapshot file does not exist!")
    return snapshot


def backup(file_dir, level, files, date):
    """
        backup files with a given level

        'file_dir' is the directory of the pos
        'level' is the backup level of the file
        'files' is an array of the folders to back up
        'date' is the current date
    """
    date_string = date.strftime(DATE_FORMAT)
    # if file exists add current and copy it., if not, don't.

    snapshot = get_snapshot_if_exists(file_dir, level)

    tar(snapshot, level, file_dir, date_string, files)


def restore(restore_dir, max_level=-1, latest_date=None):
    """
        restore files from a given level.
    """
    # Set a the max restore level to max_int
    if max_level < 0:
        max_level = sys.maxint

    # Set the latest date to now.
    if latest_date is None:
        latest_date = datetime.now()

    files = {}

    # Find all backed up files
    snapshot_number_re = re.compile("backup_level_([^._]+)_([^._]+)\.tar\.gz")
    for f in os.listdir(restore_dir):
        if fnmatch.fnmatch(f, "backup_level_*_*.tar.gz"):
            fname = os.path.basename(f)
            match = snapshot_number_re.match(fname)
            level_num = int(match.group(1))
            if level_num not in files:
                files[level_num] = {}

            date = datetime.strptime(match.group(2), DATE_FORMAT)

            if date <= latest_date:
                files[level_num][date] = f

    number_of_levels = min(len(files), max_level)

    # if levels doesn't contain all levels from 0 to max_level
    if sorted(files.keys()) != range(number_of_levels):
        raise Exception("Missing a level of backup!, levels are: {}".format(
                    sorted(files.keys())
                )
            )

    # choose the latest of all available backups
    latest_files = {}
    for level, dates in files.iteritems():
        # Get the latest from all dates.
        latest_date = sorted(dates.keys())[-1]
        latest_files[level] = dates[latest_date]

    print latest_files

    for i in range(number_of_levels):
        f_name = latest_files[i]
        # Reverse the order of files from 0 to n
        untar(restore_dir + "/" + f_name)


def run_test():

    file_1 = file_path + "/data/added_on_level_1.txt"
    file_2 = file_path + "/data/added_on_level_2.txt"

    # --- 0 ---

    print "Backing up"
    backup(file_path, 0, [file_path + "/data"], datetime.now())

    print "Adding extra file..."
    with open(file_1, "w") as f:
        f.write("Hello!")

    # --- 1 ---

    print "Backing up level 1"
    backup(file_path, 1, [file_path + "/data"], datetime.now())

    print "Adding another file, deleting previous file..."
    with open(file_2, "w") as f:
        f.write("Hello!")
    os.remove(file_1)

    # --- 2 ---

    print "Backing up level 2"
    backup(file_path, 2, [file_path + "/data"], datetime.now())

    # --- Done ---

    print "Backed up"
    print ""
    print "Okay, Restoring now..."
    restore(".")

def upload_to_glacier():
    glacier_connection = boto.connect_glacier(aws_access_key_id=ACCESS_KEY_ID,
                                    aws_secret_access_key=SECRET_ACCESS_KEY)



run_test()


# from boto.glacier.layer1 import Layer1
# from boto.glacier.vault import Vault
# from boto.glacier.concurrent import ConcurrentUploader
# import sys
# import os.path

# access_key_id = "..."
# secret_key = "..."
# target_vault_name = '...'
# fname = sys.argv[1]

# if(os.path.isfile(fname) == False):
#     print("Can't find the file to upload!");
#     sys.exit(-1);

# glacier_layer1 = Layer1(aws_access_key_id=access_key_id, aws_secret_access_key=secret_key)

# uploader = ConcurrentUploader(glacier_layer1, target_vault_name, 32*1024*1024)

# print("operation starting...");

# archive_id = uploader.upload(fname, fname)

# print("Success! archive id: '%s'"%(archive_id))