from mrjob.job import MRJob
from mrjob.step import MRStep

# To run locally: python RatingsBreakdown.py u.data
# To run with Hadoop: 
# python RatingsBreakdown.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar u.data
# The --hadoop-streaming-jar parameter is only needed with Hortonworks, but not on AWS EC2. 
# Hortonworks has problems finding where hadoop streaming lives.
# Note that in the latter command, the file is not on hdfs://, this means the command will first copy the u.data onto hdfs, and then run the jobs.

# This MRJob python script works with Java MR. Java MR will communicate data with this script through stdin/stdout.

class RatingsBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield rating, 1

    def reducer_count_ratings(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    RatingsBreakdown.run()
