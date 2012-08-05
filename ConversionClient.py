from ConversionJob import *
import socket

class ConversionServer(object):

    def __init__(self):
        self.name = socket.gethostname()

    def start(self):
        job = ConversionJob("HOLDER", self.name)
        job.find_next_job()
        job.convert()
