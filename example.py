# coding: utf-8

import os
import time
from boxsdk import Client
from auth import authenticate
from chunked_uploader import ChunkedUploader


def chunked_upload_file(client):

    def progress_callback(result, part_size, total_parts):
        part_no = result['part']['offset'] // part_size
        print('{}/{}'.format(part_no, total_parts))

    root_folder = client.folder(folder_id='0')

    file_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), 'soucle.m4v')

    chunked_uploader = ChunkedUploader(root_folder, file_path)
    parts = chunked_uploader.upload_parts(progress_callback, multi=8)
    chunked_uploader.commit(parts)

     
def run(oauth):

    client = Client(oauth)

    print('--------------------------- start ----------------------')
    start = time.time()
    chunked_upload_file(client)
    print('--------------------------- end ----------------------')
    print('{:,} [sec]'.format(time.time() - start))


def main():

    oauth, _, _ = authenticate()
    run(oauth)
    os._exit(0)

if __name__ == '__main__':
    main()
