import argparse
import cmd
import os
import os.path
import pickle
import re
import shlex
import subprocess
from itertools import takewhile
from operator import itemgetter, methodcaller
from typing import Dict
import glob

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive']
TOKEN_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'token.pickle')


class GDriveError(Exception):
    pass


class ArgparseError(Exception):
    pass


class GDriveParser(argparse.ArgumentParser):
    def error(self, message):
        raise ArgparseError(message)


def is_folder(item: Dict):
    return item['mimeType'] == 'application/vnd.google-apps.folder'


clean_name = methodcaller('replace', "'", "\\'")


get_parser = GDriveParser()
get_parser.add_argument('-r', '--recursive', action='store_true')
get_parser.add_argument('source')
get_parser.add_argument('dest', nargs='?')

rm_parser = GDriveParser()
rm_parser.add_argument('-r', '--recursive', action='store_true')
rm_parser.add_argument('source')


class GDrive(cmd.Cmd):
    intro = 'Google Drive Shell'
    prompt = '> '

    token_path = TOKEN_PATH
    service = None
    cwd = [('root', '')]

    default_list_args = {
        'pageSize': 100,
        'orderBy': 'folder,name',
        'fields': "nextPageToken, files(id,name,mimeType,parents)",
        'supportsAllDrives': True,
        'includeItemsFromAllDrives': True
    }

    def __init__(self, *args, token_path=None, **kwargs):
        super().__init__(*args, **kwargs)

        if token_path is not None:
            self.token_path = token_path

        self.login()

    def login(self):
        try:
            with open(self.token_path, 'rb') as token:
                creds = pickle.load(token)
        except (FileNotFoundError, pickle.PickleError):
            creds = None
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid or creds.scopes != SCOPES:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'client_secret.json', SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(self.token_path, 'wb') as token:
                pickle.dump(creds, token)

        self.service = build('drive', 'v3', credentials=creds)

    def preloop(self):
        user = self.service.about().get(fields='user').execute()['user']
        self.print(user['displayName'] + " — " + user['emailAddress'])

    def do_pwd(self, arg):
        print(self.make_path(), file=self.stdout)

    def do_ls(self, arg):
        cd_path = os.path.normpath(os.path.join(self.make_path(), arg)).split('/')

        same_idx = len(list(takewhile(lambda x: x[0][1] == x[1], zip(self.cwd, cd_path[:-1])))) - 1
        curr_dir = self.cwd[same_idx]
        for p in cd_path[same_idx + 1:-1]:
            try:
                curr_dir = next(map(itemgetter('id', 'name'), self.get_children(
                    curr_dir[0], name=clean_name(p), only_folders=True)))
            except StopIteration:
                self.print(f'Folder {p} does not exist')
                break

        if not cd_path[-1]:
            for item in self.get_children(curr_dir[0]):
                if is_folder(item):
                    self.print_blue(item['name'])
                else:
                    self.print(item['name'])
        else:
            try:
                curr_item = next(self.get_children(curr_dir[0], name=clean_name(cd_path[-1])))
                if is_folder(curr_item):
                    for item in self.get_children(curr_item['id']):
                        if is_folder(item):
                            self.print_blue(item['name'])
                        else:
                            self.print(item['name'])
                else:
                    self.print(curr_item['name'])

            except StopIteration:
                self.print(f'Folder {cd_path[-1]} does not exist')

    def do_cd(self, arg):
        if not arg:
            self.cwd = self.cwd[:1]
            return
        cd_path = os.path.normpath(os.path.join(self.make_path(), arg))
        cd_path = re.sub(r'\\(.)', r'\1', cd_path).split('/')

        same_idx = len(list(takewhile(lambda x: x[0][1] == x[1], zip(self.cwd, cd_path)))) - 1
        self.cwd = self.cwd[:same_idx + 1]

        for l, p in enumerate(cd_path[same_idx + 1:], same_idx):
            if not p:
                continue
            try:
                self.cwd.append(next(map(itemgetter('id', 'name'), self.get_children(
                    self.cwd[l][0], name=clean_name(p), only_folders=True))))
            except StopIteration:
                self.print(f'Folder {p} does not exist')
                break

    def complete_cd(self, text, line, begidx, endidx):
        arg = re.sub(r'^cd\s+', '', line)
        cd_path = re.sub(r'\\(.)', r'\1', os.path.normpath(os.path.join(self.make_path(), arg))).split('/')

        same_idx = len(list(takewhile(lambda x: x[0][1] == x[1], zip(self.cwd, cd_path[:-1])))) - 1
        curr_dir = self.cwd[same_idx]
        for p in cd_path[same_idx + 1:-1]:
            try:
                curr_dir = next(map(itemgetter('id', 'name'), self.get_children(
                    curr_dir[0], name=clean_name(p), only_folders=True)))
            except StopIteration:
                return []

        curr_items = [item['name'] + '/' for item in self.get_children(curr_dir[0], only_folders=True)]
        if not cd_path[-1]:
            return curr_items

        try:
            curr_item = next(self.get_children(curr_dir[0], name=clean_name(cd_path[-1]), only_folders=True))
            return [item['name'] for item in self.get_children(curr_item['id'], only_folders=True)]
        except StopIteration:
            return [curr for curr in curr_items if curr.startswith(cd_path[-1])]

    def do_get(self, arg):
        try:
            args = get_parser.parse_args(shlex.split(arg))
            cd_path = re.sub(r'\\(.)', r'\1', os.path.normpath(os.path.join(self.make_path(), args.source))).split('/')

            same_idx = len(list(takewhile(lambda x: x[0][1] == x[1], zip(self.cwd, cd_path[:-1])))) - 1
            curr_dir = self.cwd[same_idx]
            for p in cd_path[same_idx + 1:-1]:
                try:
                    curr_dir = next(map(itemgetter('id', 'name'), self.get_children(
                        curr_dir[0], name=clean_name(p), only_folders=True)))
                except StopIteration as e:
                    raise GDriveError(f'Folder {p} does not exist') from e

            if not cd_path[-1] and curr_dir[0] == 'root':
                if args.dest:
                    dest = os.path.join(args.dest, 'GDrive')
                else:
                    dest = 'GDrive'
                self.recursive_get_file('root', dest)
            try:
                item = next(self.get_children(curr_dir[0], name=clean_name(cd_path[-1])))
                if is_folder(item):
                    if not args.recursive:
                        raise GDriveError('Use recusive mode to download folder')
                    if args.dest:
                        dest = os.path.join(args.dest, item['name'])
                    else:
                        dest = item['name']

                    self.recursive_get_file(item['id'], dest)
                else:
                    self.get_file(item['id'], args.dest or os.path.basename(args.source))

            except StopIteration as e:
                raise GDriveError(f'File "{cd_path[-1]}" does not exist') from e
        except SystemExit:
            pass
        except ArgparseError as e:
            get_parser.print_usage(file=self.stdout)
            self.print(e)
        except GDriveError as e:
            self.print(e)

    def do_put(self, arg):
        try:
            args = get_parser.parse_args(shlex.split(arg))
            if args.dest:
                cd_path = re.sub(
                    r'\\(.)',
                    r'\1',
                    os.path.normpath(
                        os.path.join(
                            self.make_path(),
                            args.dest))).split('/')
            else:
                cd_path = ['']
            same_idx = len(list(takewhile(lambda x: x[0][1] == x[1], zip(self.cwd, cd_path)))) - 1
            curr_dir = self.cwd[same_idx]
            for p in cd_path[same_idx + 1:-1]:
                try:
                    curr_dir = next(map(itemgetter('id', 'name'), self.get_children(
                        curr_dir[0], name=clean_name(p), only_folders=True)))
                except StopIteration as e:
                    raise GDriveError(f'Folder {p} does not exist') from e

            uploads = glob.glob(args.source, recursive=True)

            if not uploads:
                raise GDriveError(f'"{args.source}" does not exist.')

            if any(map(os.path.isdir, uploads)) and not args.recursive:
                raise GDriveError("Use recursive mode to upload directory.")

            for g in uploads:
                self.recursive_put_file(g, curr_dir[0])

        except SystemExit:
            pass
        except ArgparseError as e:
            get_parser.print_usage(file=self.stdout)
            self.print(e)
        except GDriveError as e:
            self.print(e)

    def do_rm(self, arg):
        try:
            args = get_parser.parse_args(shlex.split(arg))
            cd_path = re.sub(
                r'\\(.)',
                r'\1',
                os.path.normpath(
                    os.path.join(
                        self.make_path(),
                        args.source))).split('/')

            same_idx = len(list(takewhile(lambda x: x[0][1] == x[1], zip(self.cwd, cd_path)))) - 1
            curr_item = self.cwd[same_idx]
            for p in cd_path[same_idx + 1:]:
                try:
                    curr_item = next(map(itemgetter('id', 'name'), self.get_children(
                        curr_item[0], name=clean_name(p), only_folders=False)))
                except StopIteration as e:
                    raise GDriveError(f'Folder {p} does not exist') from e

            item = self.service.files().get(fileId=curr_item[0], fields='id,name,mimeType').execute()

            if is_folder(item) and not args.recursive:
                raise GDriveError('Use recursive mode to delete folders')

            self.trash_file(item['id'])
            self.print(item['name'])

        except SystemExit:
            pass
        except ArgparseError as e:
            get_parser.print_usage(file=self.stdout)
            self.print(e)
        except GDriveError as e:
            self.print(e)

    def do_logout(self, arg):
        os.remove(self.token_path)
        return True

    def do_EOF(self, arg):
        self.print()
        return True

    def do_shell(self, arg):
        subprocess.call(arg, shell=True)

    def print_blue(self, line, **kwargs):
        print(f'\033[0;34m{line}\033[0m', file=self.stdout, **kwargs)

    def print(self, line='', **kwargs):
        print(line, file=self.stdout, **kwargs)

    def get_items(self, args):
        page_token = ''

        while True:
            if page_token:
                results = self.service.files().list(
                    pageToken=page_token,
                    **args).execute()
            else:
                results = self.service.files().list(
                    **args).execute()

            yield from results.get('files', [])

            try:
                page_token = results['nextPageToken']
            except KeyError:
                break

    def get_children(self, file_id, name=None, only_folders=False):
        query = f"'{file_id}' in parents and trashed = false"

        if only_folders:
            query += " and mimeType = 'application/vnd.google-apps.folder'"

        if name is not None:
            query += f" and name='{name}'"

        list_args = {
            'q': query,
            **self.default_list_args
        }

        return self.get_items(list_args)

    def get_file(self, file_id, dest):
        request = self.service.files().get_media(fileId=file_id)
        with open(dest, 'xb') as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()

            self.print(dest)

    def recursive_get_file(self, file_id, dest):
        try:
            os.mkdir(dest)
        except FileExistsError:
            pass

        self.print(dest)

        items = self.get_children(file_id)

        for item in items:
            if is_folder(item):
                self.recursive_get_file(item['id'], os.path.join(dest, item['name']))
            else:
                self.get_file(item['id'], os.path.join(dest, item['name']))

    def mkdir(self, name, parent_id=None, exist_ok=True):
        file_metadata = {
            'name': name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        if parent_id is not None:
            file_metadata['parents'] = [parent_id]

        items = list(self.get_children(parent_id or 'root', name=name, only_folders=True))

        if not exist_ok and items:
            raise GDriveError('Folder exists')

        if items:
            return items[0]

        file = self.service.files().create(body=file_metadata,
                                           fields='id,name,parents,mimeType').execute()

        return file

    def put_file(self, file, parent_id=None):
        file_metadata = {'name': os.path.basename(file)}
        if parent_id is not None:
            file_metadata['parents'] = [parent_id]
        media = MediaFileUpload(file, resumable=True)
        file = self.service.files().create(body=file_metadata,
                                           media_body=media,
                                           fields='id,name,parents,mimeType').execute()
        return file

    def recursive_put_file(self, file, parent_id=None):
        self.print(file)
        rfile = os.path.realpath(file)
        if os.path.isdir(rfile):
            parent, curr = os.path.split(rfile)
            folder = self.mkdir(curr, parent_id)
            for f in os.listdir(rfile):
                self.recursive_put_file(os.path.join(rfile, f), folder['id'])
        else:
            self.put_file(rfile, parent_id)

    def trash_file(self, file_id):
        body = {'trashed': True}
        return self.service.files().update(fileId=file_id, body=body, fields='id,name').execute()

    def make_path(self):
        return '/'.join(map(itemgetter(1), self.cwd)) + '/'


if __name__ == '__main__':
    GDrive().cmdloop()
