from ConversionJob import *

class ConversionServer(object):
    
    def __init__(self, path_movie_list, path_client_list):
        print "Generating client list"
        client_list = ConversionServer._get_list(path_client_list)
        print "Generating movie list"
        movie_list = ConversionServer._get_list(path_movie_list)
        
        print "Associating movies with clients"
        movie_client_pairs = ConversionServer._pair_client_movie(movie_list, client_list)

        print "Creating conversion jobs"
        self.jobs = ConversionServer._create_jobs(movie_client_pairs)

    def initialize_clients(self):
        print "Starting clients"
        for job in self.jobs:
            job.start_client()
        

    def start(self):
        print "Sending jobs to database"
        self._send_to_database()
        print "Sending movies to clients"
        self._send_to_clients()

    def _send_to_database(self):
        for job in self.jobs:
            job.send_to_database()
    
    def _send_to_clients(self):
        for job in self.jobs:
            job.send_to_client()

    @staticmethod
    def _create_jobs(movie_client_pairs):
        jobs = []

        for movie_client in movie_client_pairs:
            jobs.append(ConversionJob(movie_client[0], movie_client[1]))

        return jobs


    @staticmethod
    def _pair_client_movie(movie_list, client_list,):
        counter = 0
        nClients = len(client_list)

        movie_client_pairs = []
        for movie in movie_list:
            movie_client_pairs.append((movie, client_list[counter]))
            counter = (counter + 1) % nClients

        return movie_client_pairs

    @staticmethod
    def _get_list(path_list):
        f = open(path_list, 'r')
        lines = f.readlines()

        rtn_list = []
        for line in lines:
            rtn_list.append(line.rstrip())

        return rtn_list

