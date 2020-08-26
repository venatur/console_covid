import os


class CreateDir():

    def create_dir(self, dir):
        if not os.path.isdir(dir):
            os.mkdir(dir)