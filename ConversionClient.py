from ConversionJob import *
import socket

class ConversionClient(object):

    def __init__(self):
        self.name = socket.gethostname()

    def start(self):
        print "Creating new job"
        job = ConversionJob("HOLDER", self.name)
        print "Finding job"
        job.find_next_job()
        print "Converting"
        job.convert()
