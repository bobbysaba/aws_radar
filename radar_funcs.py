import pyart
import imageio
import os.path
import tempfile
from datetime import datetime
from boto.s3.connection import S3Connection

# Helper function for the search
def _nearestDate(dates, pivot):
    return min(dates, key=lambda x: abs(x - pivot))

# func to pull the data
def get_radar_data(site, datetime_t):
    # site : string of four letter radar designation (ex: 'KTLX')
    # datetime_t : datetime in which you'd like the closest scan
    # returns:
    # radar : Py-ART Radar Object of scan closest to the queried datetime

    #First create the query string for the bucket knowing
    #how NOAA and AWS store the data
    my_pref = datetime_t.strftime('%Y/%m/%d/') + site

    #Connect to the bucket
    conn = S3Connection(anon = True)
    bucket = conn.get_bucket('noaa-nexrad-level2')

    #Get a list of files
    bucket_list = list(bucket.list(prefix = my_pref))

    #we are going to create a list of keys and datetimes to allow easy searching
    keys = []
    datetimes = []

    #populate the list
    for i in range(len(bucket_list)):
        this_str = str(bucket_list[i].key)
        if 'gz' in this_str:
            endme = this_str[-22:-4]
            fmt = '%Y%m%d_%H%M%S_V0'
            dt = datetime.strptime(endme, fmt)
            datetimes.append(dt)
            keys.append(bucket_list[i])

        if this_str[-3::] == 'V06':
            endme = this_str[-19::]
            fmt = '%Y%m%d_%H%M%S_V06'
            dt = datetime.strptime(endme, fmt)
            datetimes.append(dt)
            keys.append(bucket_list[i])

    #find the closest available radar to your datetime
    closest_datetime = _nearestDate(datetimes, datetime_t)
    index = datetimes.index(closest_datetime)

    localfile = tempfile.NamedTemporaryFile()
    keys[index].get_contents_to_filename(localfile.name)
    radar = pyart.io.read(localfile.name)
    return radar

# create a function to animate figures in a directory in the order in which they were created
def gif(storm, fig_path, gif_path):
    # storm: will be used as the tile of the animation
    # fig_path: path to the directory with the figures to animate
    # gif_path: path to the directory you wish to save the animation to 
    
    # pull the files from the figure directory
    paths = [os.path.join(fig_path, file) for file in os.listdir(fig_path)]
    
    # list the times the files were created to animate in time order
    mtimes = [os.path.getmtime(os.path.join(fig_path, file)) for file in os.listdir(fig_path)]
    files_mtimes = list(zip(mtimes, paths))
    files_mtimes.sort()
    
    # create a list of the files in the correct order
    images = [imageio.imread(files_mtimes[i][1]) for i in range(0,len(files_mtimes))]
    
    # animate the images to gif
    imageio.mimsave(gif_path + storm + '.gif', images, duration = 2)
