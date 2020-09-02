from persistence.unzip_res import Unzip
import os

obj = Unzip
path_r = 'res/history/'
extension = 'zip'

result = os.listdir(path_r)
print(result)
for item in result:
    if item.endswith('.{}'.format(extension)):
            path_url = os.path.join(path_r, item)
            print(path_url)