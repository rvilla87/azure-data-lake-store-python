#!/usr/bin/env python

# -*- coding: utf-8 -*-
# coding=utf-8
# --------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

"""
An interface to be run from the command line/powershell.

This file is the only executable in the project.
"""

from __future__ import print_function
from __future__ import unicode_literals

import argparse
import cmd
from datetime import datetime
import os
import stat
import sys

from azure.datalake.store.core import AzureDLFileSystem
from azure.datalake.store.multithread import ADLDownloader, ADLUploader
from azure.datalake.store.utils import write_stdout


import configparser

config = configparser.ConfigParser()
config.read('config.properties')

tenant = config.get('Authentication', 'tenant')
auth_resource = config.get('Authentication', 'auth_resource')
client_id = config.get('Authentication', 'client_id')
client_secret = config.get('Authentication', 'client_secret')

adlsAccountName = config.get('Filesystem', 'adlsAccountName')


## Use this for Azure AD authentication
from msrestazure.azure_active_directory import AADTokenCredentials

## Required for Data Lake Storage Gen1 account management
from azure.mgmt.datalake.store import DataLakeStoreAccountManagementClient
from azure.mgmt.datalake.store.models import DataLakeStoreAccount

## Required for Data Lake Storage Gen1 filesystem management
from azure.datalake.store import core, lib, multithread

# Common Azure imports
import adal
from azure.mgmt.resource.resources import ResourceManagementClient
from azure.mgmt.resource.resources.models import ResourceGroup

## Use these as needed for your application
import logging, getpass, pprint, uuid, time

adlCreds = lib.auth(tenant_id = tenant,
                client_secret = client_secret,
                client_id = client_id,
                resource = auth_resource)

# Create filesystem client

## Create a filesystem client object
adlsFileSystemClient = core.AzureDLFileSystem(adlCreds, store_name=adlsAccountName)

## synonym for adlsFileSystemClient
afsc = adlsFileSystemClient


class AzureDataLakeFSCommand(cmd.Cmd, object):
    """Accept commands via an interactive prompt or the command line."""

    prompt = 'Azure[%s]: ' % adlsAccountName
    undoc_header = None
    _hidden_methods = ('do_EOF',)

    # If this method is not overridden, it repeats the last nonempty command entered.
    def emptyline(self):
        return ""
    
    def __init__(self, fs):
        super(AzureDataLakeFSCommand, self).__init__()
        self._fs = fs

    def get_names(self):
        return [n for n in dir(self.__class__) if n not in self._hidden_methods]

    def do_close(self, line):
        return True
        
    def help_close(self):
        print("Usage: close")
        print("Exit the application")

    def do_exit(self, line):
        return True
        
    def help_exit(self):
        print("Usage: exit")
        print("Exit the application")

    def do_cat(self, line):
        parser = argparse.ArgumentParser(prog="cat", add_help=False)
        parser.add_argument('files', type=str, nargs='+')
        try: args = parser.parse_args(line.split())
        except: pass

        for f in args.files:
            write_stdout(self._fs.cat(f))

    def help_cat(self):
        print("Usage: cat file ...")
        print("Display contents of files")

    def do_chgrp(self, line):
        parser = argparse.ArgumentParser(prog="chgrp", add_help=False)
        parser.add_argument('group', type=str)
        parser.add_argument('files', type=str, nargs='+')
        try: args = parser.parse_args(line.split())
        except: pass

        for f in args.files:
            self._fs.chown(f, group=args.group)

    def help_chgrp(self):
        print("Usage: chgrp group file ...")
        print("Change file group")

    def do_chmod(self, line):
        parser = argparse.ArgumentParser(prog="chmod", add_help=False)
        parser.add_argument('mode', type=str)
        parser.add_argument('files', type=str, nargs='+')
        try: args = parser.parse_args(line.split())
        except: pass

        for f in args.files:
            self._fs.chmod(f, args.mode)

    def help_chmod(self):
        print("Usage: chmod mode file ...")
        print("Change file permissions")

    def _parse_ownership(self, ownership):
        if ':' in ownership:
            owner, group = ownership.split(':')
            if not owner:
                owner = None
        else:
            owner = ownership
            group = None
        return owner, group

    def do_chown(self, line):
        parser = argparse.ArgumentParser(prog="chown", add_help=False)
        parser.add_argument('ownership', type=str)
        parser.add_argument('files', type=str, nargs='+')
        try: args = parser.parse_args(line.split())
        except: pass

        owner, group = self._parse_ownership(args.ownership)

        for f in args.files:
            self._fs.chown(f, owner=owner, group=group)

    def help_chown(self):
        print("Usage: chown owner[:group] file ...")
        print("Usage: chown :group file ...")
        print("Change file owner and group")

    def _display_dict(self, d):
        width = max([len(k) for k in d.keys()])
        for k, v in sorted(list(d.items())):
            print("{0:{width}} = {1}".format(k, v, width=width))

    def do_df(self, line):
        parser = argparse.ArgumentParser(prog="df", add_help=False)
        parser.add_argument('path', type=str, nargs='?', default='.')
        try: args = parser.parse_args(line.split())
        except: pass

        self._display_dict(self._fs.df(args.path))

    def help_df(self):
        print("Usage: df [path]")
        print("Display Azure account statistics of a path")

    def _truncate(self, num, fmt):
        return '{:{fmt}}'.format(num, fmt=fmt).rstrip('0').rstrip('.')

    def _format_size(self, num):
        for unit in ['B', 'K', 'M', 'G', 'T']:
            if abs(num) < 1024.0:
                return '{:>4s}{}'.format(self._truncate(num, '3.1f'), unit)
            num /= 1024.0
        return self._truncate(num, '.1f') + 'P'

    def _display_path_with_size(self, name, size, human_readable):
        if human_readable:
            print("{:7s} {}".format(self._format_size(size), name))
        else:
            print("{:<9d} {}".format(size, name))

    def do_du(self, line):
        parser = argparse.ArgumentParser(prog="du", add_help=False)
        parser.add_argument('files', type=str, nargs='*', default=[''])
        parser.add_argument('-c', '--total', action='store_true')
        parser.add_argument('-h', '--human-readable', action='store_true')
        parser.add_argument('-r', '--recursive', action='store_true')
        try: args = parser.parse_args(line.split())
        except: pass

        total = 0
        for f in args.files:
            items = sorted(list(self._fs.du(f, deep=args.recursive).items()))
            for name, size in items:
                total += size
                self._display_path_with_size(name, size, args.human_readable)
        if args.total:
            self._display_path_with_size("total", total, args.human_readable)

    def help_du(self):
        print("Usage: du [-c | --total] [-r | --recursive] [-h | --human-readable] [file ...]")
        print("Display disk usage statistics")

    def do_exists(self, line):
        parser = argparse.ArgumentParser(prog="exists", add_help=False)
        parser.add_argument('file', type=str)
        try: args = parser.parse_args(line.split())
        except: pass

        print(self._fs.exists(args.file, invalidate_cache=False))

    def help_exists(self):
        print("Usage: exists file")
        print("Check if file/directory exists")

    def do_get(self, line):
        parser = argparse.ArgumentParser(prog="get", add_help=False)
        parser.add_argument('remote_path', type=str)
        parser.add_argument('local_path', type=str, nargs='?', default='.')
        parser.add_argument('-b', '--chunksize', type=int, default=2**28)
        parser.add_argument('-c', '--threads', type=int, default=None)
        parser.add_argument('-f', '--force', action='store_true')
        try: args = parser.parse_args(line.split())
        except: pass

        ADLDownloader(self._fs, args.remote_path, args.local_path,
                      nthreads=args.threads, chunksize=args.chunksize,
                      overwrite=args.force)

    def help_get(self):
        print("Usage: get [option]... remote-path [local-path]")
        print("Retrieve the remote path and store it locally")
        print("Options:")
        print("    -b <int>")
        print("    --chunksize <int>")
        print("        Set size of chunk to retrieve atomically, in bytes.")
        print("    -c <int>")
        print("    --threads <int>")
        print("        Set number of multiple requests to perform at a time.")
        print("    -f")
        print("    --force")
        print("        Overwrite an existing file or directory.")

    def do_head(self, line):
        parser = argparse.ArgumentParser(prog="head", add_help=False)
        parser.add_argument('files', type=str, nargs='+')
        parser.add_argument('-c', '--bytes', type=int, default=1024)
        try: args = parser.parse_args(line.split())
        except: pass

        for f in args.files:
            write_stdout(self._fs.head(f, size=args.bytes))

    def help_head(self):
        print("Usage: head [-c bytes | --bytes bytes] file ...")
        print("Display first bytes of a file")

    def do_info(self, line):
        parser = argparse.ArgumentParser(prog="info", add_help=False)
        parser.add_argument('files', type=str, nargs='+')
        try: args = parser.parse_args(line.split())
        except: pass

        for f in args.files:
            self._display_dict(self._fs.info(f, invalidate_cache=False))

    def help_info(self):
        print("Usage: info file ...")
        print("Display file information")

    def _display_item(self, item, human_readable):
        mode = int(item['permission'], 8)

        if item['type'] == 'DIRECTORY':
            permissions = "d"
        elif item['type'] == 'SYMLINK':
            permissions = "l"
        else:
            permissions = "-"

        permissions += "r" if bool(mode & stat.S_IRUSR) else "-"
        permissions += "w" if bool(mode & stat.S_IWUSR) else "-"
        permissions += "x" if bool(mode & stat.S_IXUSR) else "-"
        permissions += "r" if bool(mode & stat.S_IRGRP) else "-"
        permissions += "w" if bool(mode & stat.S_IWGRP) else "-"
        permissions += "x" if bool(mode & stat.S_IXGRP) else "-"
        permissions += "r" if bool(mode & stat.S_IROTH) else "-"
        permissions += "w" if bool(mode & stat.S_IWOTH) else "-"
        permissions += "x" if bool(mode & stat.S_IXOTH) else "-"

        timestamp = item['modificationTime'] // 1000
        modified_at = datetime.fromtimestamp(timestamp).strftime('%b %d %H:%M')

        if human_readable:
            size = "{:5s}".format(self._format_size(item['length']))
        else:
            size = "{:9d}".format(item['length'])

        print("{} {} {} {} {} {}".format(
            permissions,
            item['owner'][:8],
            item['group'][:8],
            size,
            modified_at,
            os.path.basename(item['name'])))

    def do_ls(self, line):
        parser = argparse.ArgumentParser(prog="ls", add_help=False)
        parser.add_argument('dirs', type=str, nargs='*', default=[''])
        parser.add_argument('-h', '--human-readable', action='store_true')
        parser.add_argument('-l', '--detail', action='store_true')
        try: args = parser.parse_args(line.split())
        except: pass

        for d in args.dirs:
            for item in self._fs.ls(d, detail=args.detail, invalidate_cache=False):
                if args.detail:
                    self._display_item(item, args.human_readable)
                else:
                    print(os.path.basename(item))

    def help_ls(self):
        print("Usage: ls [-h | --human-readable] [-l | --detail] [file ...]")
        print("List directory contents")

    def do_mkdir(self, line):
        parser = argparse.ArgumentParser(prog="mkdir", add_help=False)
        parser.add_argument('dirs', type=str, nargs='+')
        try: args = parser.parse_args(line.split())
        except: pass

        for d in args.dirs:
            self._fs.mkdir(d)

    def help_mkdir(self):
        print("Usage: mkdir directory ...")
        print("Create directories")

    def do_mv(self, line):
        parser = argparse.ArgumentParser(prog="mv", add_help=False)
        parser.add_argument('files', type=str, nargs='+')
        try: args = parser.parse_args(line.split())
        except: pass

        self._fs.mv(args.files[0], args.files[1])

    def help_mv(self):
        print("Usage: mv from-path to-path")
        print("Rename from-path to to-path")

    def do_put(self, line):
        parser = argparse.ArgumentParser(prog="put", add_help=False)
        parser.add_argument('local_path', type=str)
        parser.add_argument('remote_path', type=str, nargs='?', default='.')
        parser.add_argument('-b', '--chunksize', type=int, default=2**28)
        parser.add_argument('-c', '--threads', type=int, default=None)
        parser.add_argument('-f', '--force', action='store_true')
        try: args = parser.parse_args(line.split())
        except: pass

        ADLUploader(self._fs, args.remote_path, args.local_path,
                    nthreads=args.threads, chunksize=args.chunksize,
                    overwrite=args.force)

    def help_put(self):
        print("Usage: put [option]... local-path [remote-path]")
        print("Store a local file on the remote machine")
        print("Options:")
        print("    -b <int>")
        print("    --chunksize <int>")
        print("        Set size of chunk to store atomically, in bytes.")
        print("    -c <int>")
        print("    --threads <int>")
        print("        Set number of multiple requests to perform at a time.")
        print("    -f")
        print("    --force")
        print("        Overwrite an existing file or directory.")

    def do_quit(self, line):
        return True

    def help_quit(self):
        print("Usage: quit")
        print("Exit the application")

    def do_rm(self, line):
        parser = argparse.ArgumentParser(prog="rm", add_help=False)
        parser.add_argument('files', type=str, nargs='+')
        parser.add_argument('-r', '--recursive', action='store_true')
        try: args = parser.parse_args(line.split())
        except: pass

        for f in args.files:
            self._fs.rm(f, recursive=args.recursive)

    def help_rm(self):
        print("Usage: rm [-r | --recursive] file ...")
        print("Remove directory entries")

    def do_rmdir(self, line):
        parser = argparse.ArgumentParser(prog="rmdir", add_help=False)
        parser.add_argument('dirs', type=str, nargs='+')
        try: args = parser.parse_args(line.split())
        except: pass

        for d in args.dirs:
            self._fs.rmdir(d)

    def help_rmdir(self):
        print("Usage: rmdir directory ...")
        print("Remove directories")

    def do_tail(self, line):
        parser = argparse.ArgumentParser(prog="tail", add_help=False)
        parser.add_argument('files', type=str, nargs='+')
        parser.add_argument('-c', '--bytes', type=int, default=1024)
        try: args = parser.parse_args(line.split())
        except: pass

        for f in args.files:
            write_stdout(self._fs.tail(f, size=args.bytes))

    def help_tail(self):
        print("Usage: tail [-c bytes | --bytes bytes] file ...")
        print("Display last bytes of a file")

    def do_touch(self, line):
        parser = argparse.ArgumentParser(prog="touch", add_help=False)
        parser.add_argument('files', type=str, nargs='+')
        try: args = parser.parse_args(line.split())
        except: pass

        for f in args.files:
            self._fs.touch(f)

    def help_touch(self):
        print("Usage: touch file ...")
        print("Change file access and modification times")

    def do_EOF(self, line):
        return True

    def do_list_uploads(self, line):
        print(ADLUploader.load())

    def help_list_uploads(self):
        print("Shows interrupted but persisted downloads")

    def do_clear_uploads(self, line):
        ADLUploader.clear_saved()

    def help_clear_uploads(self):
        print("Forget all persisted uploads")

    def do_resume_upload(self, line):
        try:
            up = ADLUploader.load()[line]
            up.run()
        except KeyError:
            print("No such upload")

    def help_resume_upload(self):
        print("Usage: resume_upload name")
        print()
        print("Restart the upload designated by <name> and run until done.")

    def do_list_downloads(self, line):
        print(ADLDownloader.load())

    def help_list_downloads(self):
        print("Shows interrupted but persisted uploads")

    def do_clear_downloads(self, line):
        ADLDownloader.clear_saved()

    def help_clear_downloads(self):
        print("Forget all persisted downloads")

    def do_resume_download(self, line):
        try:
            up = ADLDownloader.load()[line]
            up.run()
        except KeyError:
            print("No such download")

    def help_resume_download(self):
        print("Usage: resume_download name")
        print()
        print("Restart the download designated by <name> and run until done.")

    def do_find(self, line):
        parser = argparse.ArgumentParser(prog="find", add_help=False)
        parser.add_argument('path', type=str, nargs='?', default='.')
        parser.add_argument('partial_filename', type=str, nargs='?')
        try: args = parser.parse_args(line.split())
        except: pass

        for filename in self._fs.glob(args.path):
            if not args.partial_filename:
                print(filename)
            else:
                if args.partial_filename in filename:
                    print(filename)

    def help_find(self):
        print("Usage: find path [partial_filename]")
        print("Find partial filename in a path (recursively)")


if __name__ == '__main__':
    #fs = AzureDLFileSystem()
    fs = afsc
    
    print("""Welcome to the Azure Datalake Store CLI!

Type '?' or 'help' for listing all the available commands and 'help command' in order to print the command usage.

For exit, type 'exit' or 'close'.
""")

    if len(sys.argv) > 1:
        AzureDataLakeFSCommand(fs).onecmd(' '.join(sys.argv[1:]))
    else:
        while True:
            try:
                AzureDataLakeFSCommand(fs).cmdloop()
            except UnboundLocalError:
                pass
            except AttributeError:
                print(" >> AttributeError: %s" % sys.exc_info()[1])
                pass
            except Exception as ex:
                print(" >> Exception: '%s: %s'" % (type(ex).__name__, ex))
                pass

