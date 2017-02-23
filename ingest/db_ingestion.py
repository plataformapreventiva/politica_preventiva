"""
This is a commandline script that can be used to ingest all the raw data
from into a postgres schema. For description of the input required for
this script type python3 db_ingestion.py -h in the command line. The most common
use case will be calling

"""

# Setup utility functions and password / configuration info
conf = {}
with open("../conf/db_profile.yaml", "r") as f:
    db_profile = yaml.load(f)
    conf["USER"] = db_profile["PGUSER"]
    conf["PASSWORD"] = db_profile["PGPASSWORD"]

