import MySQLdb
import re
import os
import time

class ConversionJob(object):
    """Represents a single movie to be converted on a single client"""

    """Constants for state"""
    ST_TRANSFER = 0
    ST_READY = 1
    ST_WORKING = 2
    ST_FINISHED = 3

    def __init__(self, movie, client):
        self.movie = re.escape(movie)
        self.client = client

    def _connect(self):
        host = "68.48.166.57"
        username = "root"
        password ="myr00t"
        database = "DistCon"

        self.con = MySQLdb.connect(host, username, password, database)
        self.cursor = self.con.cursor()

    def _disconnect(self):
        self.con.commit()
        self.con.close()

    def find_next_job(self):        
        print "connecting"
        self._connect()
        print "connected"
        job_command = "SELECT `movie` FROM `DistCon`.`Conversions` WHERE `client`='{host_name}' AND `state`='{state}';".\
                    format(host_name = self.client, state = ConversionJob.ST_READY)
        print job_command

        while True:
            print "Looking for job"
            if self.cursor.execute(job_command) > 0:
                self.movie = re.escape(self.cursor.fetchone()[0])
                break
            time.sleep(1)

        self._disconnect()

    def send_to_database(self):
        self._connect()
        new_conversion = """INSERT INTO `DistCon`.`Conversions` (movie, client) VALUES ('%s', '%s');"""
        self.cursor.execute(new_conversion % (self.movie, self.client))
        self._disconnect()

    def send_to_client(self):
        copy_command = "scp -q {movie} pes014@{client}:/tmp/".\
                format(movie = self.movie, client = self.client)

        os.system(copy_command)

        self.update_status(ConversionJob.ST_READY)
    
    def update_status(self, status):
        self._connect()
        update_command = """UPDATE `DistCon`.`Conversions`
                            SET `state`={new_status}
                            WHERE `movie`='{movie}' AND `client`='{client}';""".\
                         format(new_status = status, movie = self.movie, client = self.client)

        self.cursor.execute(update_command)

        self._disconnect()

    def convert(self):
        self.update_status(ConversionJob.ST_WORKING)
        copy_command = "mv /tmp/{avi} /tmp/{webm}".format(avi = self.movie, webm = self.movie[:-3] + "webm")
        os.system(copy_command)
 
        self.update_status(ConversionJob.ST_FINISHED)

    def start_client(self):
        start_command = "ssh pes014@{host_name} 'nohup python ~/DistCon/Client.py'".\
                format(host_name = self.client)
        os.system(start_command)
        print "    Started client on %s" % self.client
        
        


        
