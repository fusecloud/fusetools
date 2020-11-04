from indeed import IndeedClient


class Indeed:

    @classmethod
    def search_jobs(cls, api_key, location, query_term, max_results=25, start_pos=0):
        """

        """
        client = IndeedClient(api_key)
        params = {
            'q': query_term,
            'l': location,
            'userip': "1.2.3.4",
            'useragent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            'sort': "date",
            'limit': max_results,
            'start': start_pos,
        }

        res = client.search(**params)
        return res


class Upwork:
    # https://developers.upwork.com/?lang=python#getting-started
    pass


class Freelancer:
    # https://github.com/freelancer/freelancer-sdk-python
    pass
