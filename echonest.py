import os
import urllib
import urllib2
import pandas as pd
from joblib import Parallel, delayed
import xml.etree.cElementTree as ET


API_KEY = os.environ.get('ECHONEST_API_KEY')

def _get_root_xml(web_page):
	response = urllib2.urlopen(web_page)
	tree = ET.parse(response)
	root = tree.getroot()
	return root


def master_mood_list(word_file, word_column, api_key=API_KEY):
    """
    Function to get words that are in both options for queries in Echo Nest searches as well as
    words that are in some word list that measures emotion and energy for each word.

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

