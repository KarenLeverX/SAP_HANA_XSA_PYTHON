from hdbcli import dbapi
import os
def get_connection():
    """
Option 1, retrieve the connection parameters from the hdbuserstore
key='USER1UserKey'

Option2, specify the connection parameters
dbapi.connect( address= '', port='000', user='', password='00000', )

Additional parameters
encrypt=True, # must be set to True when connecting to HANA as a Service
As of SAP HANA Client 2.6, connections on port 443 enable encryption by default (HANA Cloud)
sslValidateCertificate=False #Must be set to false when connecting
to an SAP HANA, express edition instance that uses a self-signed certificate.
    """
    try:
     conn = dbapi.connect(
                           address='192.168.132.2',
                           port='30013',
                           user= os.environ['HANA_USER'],
                           password= os.environ['HANA_PASSWORD'],
                           database = 'SYSTEMDB'
                          )
    except:
     print("Oops!  Problem with connection.... Check it!")
    return conn