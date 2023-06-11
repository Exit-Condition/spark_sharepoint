import sharepy
from sharepy import connect
from sharepy import SharePointSession

"""
### Setup ###
1. Set up Microsoft Sandbox env - https://www.sharepointdiary.com/2021/05/how-to-create-sharepoint-online-free-trial-account.html
2. Disable Security Defaults - https://learn.microsoft.com/en-gb/azure/active-directory/fundamentals/concept-fundamentals-security-defaults
"""

SPUrl = "7hyxw3.sharepoint.com"
username = 'admin@7hyxw3.onmicrosoft.com'
password = 'ZQ7aw5gJZRSgy7m'

s = sharepy.connect(SPUrl, username, password)

if not hasattr(s, 'cookies'):
    print("Authentication Failed!")
    quit()
else:
    site = "https://7hyxw3.sharepoint.com/sites/SparkInput/"

    # API / MicroServices
    # EndPoint(URL), Request and Response

    # Get file content as stream
    r = s.get(site + 'Shared%20Documents/test_sharepoint.csv')
    print(r.status_code)    # status_code is 200 for a successful call
    if r.status_code == 200:
        print(r.content)
    else:
        print("Failed to download the file content!")

    # Download and save file on disc
    r = s.getfile(site + 'Shared%20Documents/test_sharepoint.csv', filename='C:\\Users\\parix\\Downloads\\SharePoint\\out_test_sharepoint.csv')
    print(r.status_code)

    if r.status_code == 200:
        print("File successfully downloaded!")
    else:
        print("Failed to download the file!")

print("Script Completed!")
