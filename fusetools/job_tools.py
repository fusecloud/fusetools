"""
Job Tools.

|pic1|
    .. |pic1| image:: ../images_source/indeed.png
        :width: 45%
"""

from indeed import IndeedClient


class Indeed:
    """
    Functions for interacting with Indeed.

    .. image:: ../images_source/indeed.png
    """

    @classmethod
    def search_jobs(cls, api_key, location, query_term, max_results=25, start_pos=0):
        """
        Search jobs on Indeed.

        :param api_key: Indeed API Key.
        :param location: Location (ex: city).
        :param query_term: Job term to search for.
        :param max_results: Maximum results to pull (limit is 25).
        :param start_pos: Paginated record position to start query from.
        :return: Indeed API query results.
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
