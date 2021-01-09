"""
Functions for interacting with Music Tools.

|pic1|
    .. |pic1| image:: ../images_source/music_tools/spotify1.png
        :width: 45%

"""

from spotipy.oauth2 import SpotifyClientCredentials
import spotipy


class Spotify:
    """
    Functions for interacting with Spotify.

    .. image:: ../images_source/music_tools/spotify1.png
    """

    @classmethod
    def auth(cls, clientid, clientsecret):
        """
        Creates a Spotify API authentication object.

        :param clientid: Spotify developer client Id.
        :param clientsecret: Spotify developer client secret.
        :return: Spotify API authentication object.
        """
        client_credentials_manager = SpotifyClientCredentials(client_id=clientid, client_secret=clientsecret)
        sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
        return sp

    @classmethod
    def get_playlist_tracks(cls, sp, username, playlist_id):
        """
        Retrieve tracks for a given Spotify playlist.

        :param sp: Spotify API authentication object.
        :param username: Spotify username.
        :param playlist_id: Spotify playlist Id.
        :return: List of tracks on a Spotify playlist.
        """
        results = sp.user_playlist_tracks(username, playlist_id)
        tracks = results['items']
        while results['next']:
            results = sp.next(results)
            tracks.extend(results['items'])
        return tracks

    @classmethod
    def get_track_audio_features(cls, sp, track_id):
        """
        Retrieve audio feature tracks for a given track.

        :param sp: Spotify API authentication object.
        :param track_id: Spotify track Id.
        :return: Audio feature tracks for a given track.
        """
        results = sp.audio_analysis(track_id=track_id)
        return results
