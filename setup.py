from distutils.core import setup
import py2exe
import sys 
sys.setrecursionlimit(1000000) #例如这里设置为一百万

setup(windows=['opc2influxdb.py'])
