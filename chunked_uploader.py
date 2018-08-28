# coding: utf-8

import os
import datetime
import json
import mmap
import base64
import contextlib
import concurrent.futures
from operator import itemgetter
from hashlib import sha1
from boxsdk.config import API

class ChunkedUploader:

    class UploadSession:

        def __init__(self, data):
            """ data example.
            {
                'id': 'F162E0F26861C4D25AB1ED3D82FCA86A', 
                'total_parts': 30, 
                'session_endpoints': {
                    'abort': 'https://upload.box.com/api/2.0/files/upload_sessions/F162E0F26861C4D25AB1ED3D82FCA86A', 
                    'log_event': 'https://upload.box.com/api/2.0/files/upload_sessions/F162E0F26861C4D25AB1ED3D82FCA86A/log', 
                    'commit': 'https://upload.box.com/api/2.0/files/upload_sessions/F162E0F26861C4D25AB1ED3D82FCA86A/commit', 
                    'upload_part': 'https://upload.box.com/api/2.0/files/upload_sessions/F162E0F26861C4D25AB1ED3D82FCA86A', 
                    'list_parts': 'https://upload.box.com/api/2.0/files/upload_sessions/F162E0F26861C4D25AB1ED3D82FCA86A/parts', 
                    'status': 'https://upload.box.com/api/2.0/files/upload_sessions/F162E0F26861C4D25AB1ED3D82FCA86A'
                }, 
                'session_expires_at': '2018-08-29T02:29:49Z', 
                'num_parts_processed': 0, 
                'type': 'upload_session', 
                'part_size': 16777216
            }
            """
            self._data = data

        def items(self):
            return self._data.items()

        def keys(self):
            return self._data.keys()

        def values(self):
            return self._data.values()

        def __getattr__(self, key):
            val = self._data[key] if key in self._data.keys() else None
            if isinstance(val, dict):
                val = ChunkedUploader.UploadSession(val)
            return val

    def __init__(self, folder, file_path, *, file_name=None):

        self._folder = folder
        self._file_path = file_path
        self._file_size = os.path.getsize(file_path)
        self._file_name = file_name if file_name else os.path.basename(file_path)
        self._upload_session = self._create_upload_session()
        
    def _create_upload_session(self):

        url = '{0}/files/upload_sessions'.format(API.UPLOAD_URL)
        data = {
            'folder_id': self._folder._object_id,
            'file_size': os.path.getsize(self._file_path),
            'file_name': self._file_name
        }
        print('URL:{}'.format(url))
        params = {
            'data': json.dumps(data),
            'headers': {
                'content-type': 'application/json'
            }
        }
        box_response = self._folder._session.post(url, **params)
        return ChunkedUploader.UploadSession(box_response.json())

    def _multi_upload_part(self, params_list, multi, progress_callback):

        url = self._upload_session.session_endpoints.upload_part
        put = self._folder._session.put

        total_parts = self._upload_session.total_parts
        part_size = self._upload_session.part_size
        results = []

        def progress(f):
            result = f.result().json()
            progress_callback(result, part_size, total_parts)
            return result
    
        with concurrent.futures.ThreadPoolExecutor(max_workers=multi) as _:
            futures = [ _.submit(put, url, **p) for p in params_list]
            results = [
                progress(f) for f in concurrent.futures.as_completed(futures)
            ]

        return sorted(results, key=lambda x: x['part']['offset'])

    def _upload_part(self, m, progress_callback, multi=4):

        part_size = self._upload_session.part_size
        beg, end, size = 0, part_size, self._file_size

        params_list = [] 
        for part in range(self._upload_session.total_parts):
            data = m[beg:end]
            digest = base64.b64encode(sha1(data).digest()).decode('utf-8')
            params = {
                'data': data,
                'headers': {
                    'content-range': 'bytes {}-{}/{}'.format(beg, end-1, size),
                    'content-type': 'application/octet-stream',
                    'digest': 'sha={}'.format(digest),
                }
            }
            params_list.append(params)
            beg = end
            end = beg + part_size
            end = size if end > size else end

        results = self._multi_upload_part(params_list, multi, progress_callback)
        return results

    def commit(self, parts):

        url = self._upload_session.session_endpoints.commit

        digest = parts.pop('digest')
        params = {
            'data': json.dumps(parts),
            'headers': {
                'content-type': 'application/json',
                'digest': 'sha={}'.format(digest)
            }
        }
        self._folder._session.post(url, **params)

    def upload_parts(self, progress_callback, multi=4): 

        results = None
        now = datetime.datetime.utcnow()
        with open(self._file_path, 'r+') as f:
            with contextlib.closing(mmap.mmap(f.fileno(), 0)) as m:
                digest = base64.b64encode(sha1(m).digest()).decode('utf-8')
                results = self._upload_part(m, progress_callback, multi)
                parts = {
                    'digest': digest,
                    'parts': [ x['part'] for x in results ],
                    #'attributes': {
                    #    'content_modified_at': RFC 3339 formated date 
                    #}
                }
        return parts

