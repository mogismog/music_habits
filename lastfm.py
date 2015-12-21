import os
import urllib2
import pandas as pd
from joblib import Parallel, delayed
import xml.etree.cElementTree as ET


API_KEY = os.environ.get('LASTFM_API_KEY')

USER_NAME = os.environ.get('LASTFM_USER_NAME')

BASE_WEB_PAGE = ''.join(["http://ws.audioscrobbler.com/2.0/?",
                    "method=user.getrecenttracks&user={}&api_key={}".format(USER_NAME, API_KEY)])


def _get_root_xml(web_page):
	response = urllib2.urlopen(web_page)
	tree = ET.parse(response)
	root = tree.getroot()
	return root


def lastfm_xml_to_df(web_page):
	"""Scrape XML from last.fm and put into pandas DataFrame."""

	root = _get_root_xml(web_page)
	return_list = []
	for track in root.iter('track'):
		temp_dict = {'track_mbid': track.find('mbid').text,
		             'artist': track.find('artist').text,
		             'artist_mbid': track.find('artist').attrib['mbid'],
		             'track_name': track.find('name').text,
		             'timestamp': pd.to_datetime(int(track.find('date').attrib['uts']), unit='s')}
		return_list.append(temp_dict)
	return pd.DataFrame(return_list)


def get_total_pages(web_page, limit=500):
	"""Helper function to find total number of XML
	   pages of scrobbles given a specific limit."""

	root = _get_root_xml(web_page + '&limit={}'.format(limit))
	return int(root.find('recenttracks').attrib['totalPages'])


def grab_all_scrobbles(web_page=BASE_WEB_PAGE, limit=500, saved_df_name=None,
	                   n_proc=-1):
	"""Function to parse Last.fm scrobble pages.

	This function takes a string of a url that points to last.fm's API
	in order to grab recent tracks. The url ought to look like:

	http://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user=some_user_name&api_key=some_key

	We can then grab all of our scrobbles from last.fm!

	Parameters
	----------
	web_page : str
	    Base webpage pointing to last.fm's API service that will
	    get recent tracks. The default pulls information from the
	    environmental variables LASTFM_API_KEY and LASTFM_USER_NAME
	    in order to fill out the important parts.
	limit : int
	    The number of scrobbles per page on last.fm's API. (default: 500)
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

	# In order to do multiprocessing, we need to get the total
	# number of pages to scrape. This changes with different limits.
	total_pages = get_total_pages(web_page, limit=limit)

	# Scrape all the XML data and throw into a list of pandas DataFrames
	list_of_dfs = Parallel(n_proc)(delayed(lastfm_xml_to_df)
		(web_page + '&limit={}&page={}'.format(limit, p)) for p in xrange(1, total_pages + 1))

	# concat!
	df = pd.concat(list_of_dfs)

	# Save (if applicable)
	if saved_df_name:
		df.to_pickle(saved_df_name)

	return df
