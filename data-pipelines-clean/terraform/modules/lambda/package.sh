mkdir python

python3 -m pip install -r /io/requirements.txt -t ./python

zip -r /io/${ARCHIVE_NAME} ./python/