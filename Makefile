
# This will fail if pythonpath has more than one path
PYLIBDIR = $(PYTHONPATH)

default: install

install: mabaker.py
	cp $^ $(PYLIBDIR)
