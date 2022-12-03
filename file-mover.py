import os
import glob

paths = glob.glob('./**/*', recursive = True)
paths = [os.path.abspath(path) for path in paths]
paths = list(filter(lambda path: os.path.isfile(path), paths))
for file in paths:
    os.system(f'mv {file} .')