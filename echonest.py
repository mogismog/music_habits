import os
import time
import urllib
import urllib2
import pandas as pd
from joblib import Parallel, delayed
import xml.etree.cElementTree as ET

# pyechonest imports
from pyechonest import song, config


ECHONEST_API_KEY = os.environ.get('ECHONEST_API_KEY')

SUMMARY_KEYS = ['energy', 'valence', 'tempo', 'liveliness',
                'danceability', 'loudness', 'acousticness']

def _get_root_xml(web_page):
    response = urllib2.urlopen(web_page)
    tree = ET.parse(response)
    root = tree.getroot()
    return root


def master_mood_list(word_file, word_column,
                     api_key=ECHONEST_API_KEY):
    """
    Function to get words that are in both options for queries in
    Echo Nest searches as well as words that are in some word list that
    measures emotion and energy for each word.

    Parameters
    ----------

    word_file : str
        File name of analyzed word list
    word_column : str
        Name of the column in word_file that has the list of words.
    api_key : str
        Echo Nest API key. By default, this pulls the key from
        the environmental variable ECHONEST_API_KEY

    Returns
    -------
    word_df : pandas DataFrame
        A subset of word_file that only includes moods available
        in EchoNest queries.
    """
    # Set up dictionary to make this easier
    url_dict = {'type' : 'mood',
                'api_key': api_key,
                'format' : 'xml'}

    # Open up web page and scrape
    base_web_page = 'http://developer.echonest.com/api/v4/artist/list_terms?'
    root_url = '{}&{}'.format(base_web_page, urllib.urlencode(url_dict))
    root = _get_root_xml(root_url)
    echonest_moods = []

    # Get Echo Nest moods
    for mood in root.iter('terms'):
        echonest_moods.append(mood.find('name').text)

    # Now, open up word analysis file, put into pandas DataFrame
    word_df = pd.read_csv(word_file, header=0)

    # Only keep rows whose word is an option in EchoNest
    word_df = word_df.loc[word_df[word_column].isin(echonest_moods)]

    return word_df


def grab_song_summary(track_name, artist_name, api_key=ECHONEST_API_KEY,
                      studio_only=True, rest_time=30, search_kwds={}):
    """
    Function to get audio summary of

    Parameters
    ----------

    track_name : str
        Song title to search.
    artist_name : str
        Name of the band that performed track_name
    api_key : str
        Echo Nest API key. By default, this pulls the key from
        the environmental variable ECHONEST_API_KEY
    sudio_only : bool
        Set to only look for songs recorded in the studio (e.g. not including
        bootlegs or live performances with audience noise.)
    rest_time : int
        Number of seconds to pause after looking up a song. EchoNest gives us
        a very finite number of API calls per minute (They say 120 but it is
        more like 20 in my experience). As such, you may want to pause after
        each call so as to not get errors.
    search_kwds : dict
        Other keyword arguments to pass into the search API.


    Returns
    -------
    summary : dict
        A dictionary of the audio summary of each song, including energy,
        valence, etc.
    """

    # configure pyechonest by adding in the passed API key
    if not config.ECHO_NEST_API_KEY:
        config.ECHO_NEST_API_KEY = api_key

    # keyword arguments passed to pyechonest
    echo_kwargs = {'artist': artist_name,
                   'title': track_name,
                   'rank_type': 'relevance',
                   'results': 1}

    if studio_only:
        echo_kwargs.update({'song_type': 'studio'})

    # Search for the song data on echonest and get the audio summary
    search_kwds.update(echo_kwargs)
    try:
        summary = song.search(**search_kwds)[0].audio_summary
    except:
        summary = None
    finally:
        summary.update(echo_kwargs)
        time.sleep(rest_time)
        return summary


def parse_echonest(artists, tracks, saved_df_name=None, n_proc=-1,
                   studio_only=True, rest_time=30, search_kwds={},
                   api_key=ECHONEST_API_KEY):
    """Function to parse Last.fm scrobble pages.

    This function takes a string of a url that points to last.fm's API
    in order to grab recent tracks. The url ought to look like:

    http://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user=some_user_name&api_key=some_key

    We can then grab all of our scrobbles from last.fm!

    Parameters
    ----------

    saved_df_name : str
        If set, the resulting pandas DataFrame will be saved to this
        path. (default: None)
    n_proc : int
        Number of processors to use when scraping last.fm's data. If
        set to -1, the function uses all processors available.

    Returns
    -------
    df : pandas DataFrame
        DataFrame with last.fm scrobbles information.
    """


    # Scrape all the XML data and throw into a list of dictionaries
    list_of_dicts = Parallel(n_proc)(delayed(grab_song_summary)
        (track, artist, api_key, studio_only, rest_time, search_kwds)
         for artist, track in zip(artists, tracks))

    # filter dicts
    list_of_dicts = filter(None, list_of_dicts)

    # put dictionaries into pandas DataFrame
    df = pd.DataFrame(list_of_dicts)

    # Save (if applicable)
    if saved_df_name:
        df.to_pickle(saved_df_name)

    return df