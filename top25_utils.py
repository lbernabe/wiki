# -*- coding: utf-8 -*-
"""
Created on Wed May 12 07:59:04 2021
Utilities for the top_25 script. 
a) Funcion
b) Creat uigi pipeline
@author: lbernabe
"""

import requests
import os
import pandas as pd 


def getAndProcess_WikiDumps_hour(target_time, outpath):
    """
    This function executes 3 task: 
    - get input dumps
    - process
    - save output files.
    
    """
    
    # Setting up location path of the file
    # ${YEAR}/${YEAR}-${MONTH}/projectcounts-${YEAR}${MONTH}${DAY}-${HOUR}0000
    
    year = target_time.year
    month = target_time.month
    day = target_time.day
    hour = target_time.hour
    
    # This pageviews_url can be an input parameter for the function
    pageviews_url = 'https://dumps.wikimedia.org/other/pageviews/'
    pageviews_url_path = os.path.join(pageviews_url, '{}/'.format(year))
    pageviews_url_path = os.path.join(pageviews_url_path, '{}-{}/'.format(year,str(month).zfill(2)))
    pageviews_file_name = 'pageviews-'+str(year)+str(month).zfill(2)+str(day).zfill(2)+'-'+str(hour).zfill(2)+'0000.gz'
    pageviews_file_path = os.path.join(pageviews_url_path, pageviews_file_name)
    
    print('Downloaded: '+ pageviews_file_path)

    # Request data from wiki url dump
    r = requests.get(pageviews_file_path, allow_redirects=True)
    
    if r.status_code != 200:
        # This means something went wrong.
        raise ValueError("Unable to query dump for " + pageviews_file_name)
        return
    
    # Safe the file locally 
    open(pageviews_file_name, 'wb').write(r.content)
    wiki_df = pd.read_csv(pageviews_file_name, compression='gzip', header = None, sep=' ', quotechar='"', error_bad_lines=False)
    col_list =['domain', 'page', 'count', 'response_size']
    wiki_df.columns = col_list
    
    # Load the black list pages
    blacklist_df = pd.read_csv('blacklist_domains_and_pages', header = None, sep=' ', quotechar='"', error_bad_lines=False)
    col_list2 =['domain', 'page']
    blacklist_df.columns = col_list2
    
    # Remove the list from the black list
    # Using method 1 as it is faster (from the above cells and jupyter notebook)
    wiki_clean_df = wiki_df.merge(blacklist_df, indicator = True, how = 'outer')
    wiki_clean_df = wiki_clean_df.query('_merge=="left_only"').drop('_merge', axis = 1)
    
    # Sort by domain (ascending) and counts (nlargest sorts in decending order)
    wiki_clean_df.sort_values(['domain'], ascending=True, inplace = True)
    top25_df = wiki_clean_df.groupby(['domain'], sort=False).apply(lambda x: x.nlargest(25,'count')).reset_index(drop=True)
    
    #Safe to a local file
    top25_file_name = 'top25-'+str(year)+str(month).zfill(2)+str(day).zfill(2)+'-'+str(hour).zfill(2)+'0000'
    top25_file_path = os.path.join(outpath, top25_file_name)
    top25_df.to_csv(top25_file_path)
    
    
    
    
    
    