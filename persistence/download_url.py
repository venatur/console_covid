import requests
import time
from logger import Logger


class DownloadUrl:

    def download(self, url, save_path):
        obj_log = Logger()
        t = time.process_time()
        try:
            r = requests.get(url)
            path = save_path
            chunk_size = 128
            if r.status_code == 200:
                with open(path, 'wb') as fd:
                    for chunk in r.iter_content(chunk_size=chunk_size):
                        fd.write(chunk)
                elapsed_t = time.process_time() - t
                obj_log.log('descarga realizada', elapsed_t)
            else:
                print("not found "+url)
        except requests.exceptions.RequestException as error:
            print(error)



