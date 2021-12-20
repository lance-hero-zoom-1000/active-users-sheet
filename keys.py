import os

# FOR PERSONAL LAPTOPS
DINEOUT_DB_CREDENTIALS = "/users/lawrence.veigas/downloads/projects/creds/creds.json"
GOOGLE_OAUTH_CREDENTIALS = (
    "/users/lawrence.veigas/downloads/projects/creds/google_credentials.json"
)
GADINEOUT_SERVICE_ACCOUNT = (
    "/users/lawrence.veigas/downloads/projects/creds/ga_sa_account.json"
)
DP_DUMPS_PATH = "/users/lawrence.veigas/downloads/projects/ga_dashboard/dumps"

# FOR VM
# DINEOUT_DB_CREDENTIALS = "/home/lawrence.veigas/projects/creds/creds.json"
# GOOGLE_OAUTH_CREDENTIALS = (
#     "/home/lawrence.veigas/projects/creds/google_credentials.json"
# )
# GADINEOUT_SERVICE_ACCOUNT = "/home/lawrence.veigas/projects/creds/ga_sa_account.json"
# DP_DUMPS_PATH = "/home/lawrence.veigas/projects/ga_dashboard/dumps"

os.environ["DINEOUT_DB_CREDENTIALS"] = DINEOUT_DB_CREDENTIALS
os.environ["GOOGLE_OAUTH_CREDENTIALS"] = GOOGLE_OAUTH_CREDENTIALS
os.environ["GADINEOUT_SERVICE_ACCOUNT"] = GADINEOUT_SERVICE_ACCOUNT
os.environ["DP_DUMPS_PATH"] = DP_DUMPS_PATH
