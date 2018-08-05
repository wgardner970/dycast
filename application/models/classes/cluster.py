

class Cluster(object):

    def __init__(self):
        self.point = None
        self.cases = None
        self.case_count = None
        self.close_in_space = None
        self.close_in_time = None
        self.close_space_and_time = None
        self.cumulative_probability = None

    def get_case_count(self):
        if self.cases is not None:
            return len(self.cases)
