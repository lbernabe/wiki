# -*- coding: utf-8 -*-
"""
Created on Thu May 13 20:56:31 2021
Move the functionality of the Jupyter notebook into scripts and using pipeline functionality. 

@author: lbernabe
"""
import top25_utils
import os
import argparse

from datetime import datetime
from datetime import timedelta




if __name__ == '__main__':
    
    
    parser = argparse.ArgumentParser(description='')
    
    
    parser.add_argument('-s', '--startdate',
                        dest='startdatehour',
                        help='Start date and hour in the following format %Y%m%d-%H. If not populated, it will return statistics on the current hour',
                        required=False)
    parser.add_argument('-e', '--enddate',
                        dest='enddatehour',
                        help='End date and hour in the following format %Y%m%d-%H. If not populated, it will return statistics on the start/current hour',
                        required=False)
    parser.add_argument('-d', '--destination',
                        dest='destination',
                        help='Destination to place Top25 files (S3/local)',
                        required=False)

    args = vars(parser.parse_args())
    print('Params')
    print(args)
    working_dir = args['destination']
    start_time = args['startdatehour']
    end_time = args['enddatehour']
    
    f = "%Y%m%d-%H"
    hour = timedelta(hours=1)
        
    # Variables manipulation
    
    if working_dir is None:
        print ("No destination provided for top25 files, using: {}".format(os.getcwd()))
        working_dir = os.getcwd()
    else: 
        print('Destination folder:{}'.format(working_dir))
        
    if start_time is None:
        print ("No start date provided, using: {}".format(datetime.now().strftime(f)))
        start_time = datetime.now().strftime(f)
    else:
        print('Start time: {}'.format(start_time))
        
    if end_time is None:
        print ("No end time provided, only analyzing one hour time frame")
    else:
        print('End time: {}'.format(end_time))
        
    
    if end_time is None: 
            # If not date is entered, we use current date
            target_time = datetime.strptime(start_time, f)
            top25_file_name = 'top25-'+target_time.strftime(f)+'0000'
         
            if top25_file_name not in os.listdir(working_dir): 
                top25_utils.getAndProcess_WikiDumps_hour(target_time, outpath = working_dir)
                print (target_time.strftime(f) + ' processed')
            else: 
                print ('Skip '+ target_time.strftime(f) + ' already processed see '+ str(working_dir))
        
    else:
            # Loop for staring and end point
            target_time = datetime.strptime(start_time, f)
            target_endtime = datetime.strptime(end_time, f)
            
            if target_endtime < target_time:
                raise ValueError("End time cannot be smaller than start time!")
                
            while target_time <= target_endtime:
                
                top25_file_name = 'top25-'+target_time.strftime(f)+'0000'
                # Only run if data/hour has not been processed before
                if top25_file_name not in os.listdir(working_dir): 
                    top25_utils.getAndProcess_WikiDumps_hour(target_time, outpath = working_dir)
                    print (target_time.strftime(f) + ' processed')
                else: 
                    print ('Skip '+ target_time.strftime(f) + ' already processed see '+ str(working_dir))
                target_time += hour

   