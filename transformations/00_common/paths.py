BASE_PATH = "s3://ecom-pr1/raw/incremental_load/"

def get_staging_path(table):
    return f"{BASE_PATH}{table}/staging/"