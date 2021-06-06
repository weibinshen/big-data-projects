from mrjob.job import MRJob
from mrjob.step import MRStep

class MoviePopularitySort(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_movie_ids,
                   reducer=self.reducer_count_movie_ids),
            MRStep(reducer=self.reducer_sorted_output)
        ]

    def mapper_get_movie_ids(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_count_movie_ids(self, movie, ones):
        # A little bit of hack here...
        # Because we are using hadoop streaming, all values are coming in as strings
        # So in order to make sure "4" is actually sorted before "10"
        # We zero-fill the values
        # We also flip the key-value here so the subsequent reducer can sort for us.
        yield str(sum(ones)).zfill(5), movie

    def reducer_sorted_output(self, count, movies):
        # We use a for loop here because it could be possible that two movies gets the same count.
        for movie in movies:
            yield movie, count

if __name__ == '__main__':
    MoviePopularitySort.run()
