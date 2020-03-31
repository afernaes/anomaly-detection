def downloadAemetObservation():
    import os, uuid
    from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
    connect_str= "DefaultEndpointsProtocol=https;AccountName=stessapocaemet;AccountKey=B9bCFqQ/+VXHBKl5yQjo19McC8zK1lqYxkz92jx+wHcYIPFn+wELpi8wI/4U1EECguz/at8CeJJy5kUOa8sQmQ==;EndpointSuffix=core.windows.net"
    # Your Container name
    container_name = "aemet"
    # Local data path
    local_path = "./data/blob"
    # Upload file name

    # Get Azure account objects

    # Get container
    container = ContainerClient.from_connection_string(connect_str, container_name)
    # Create the BlobServiceClient object which will be used to create a container client
    list_files = []
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    for x in container.list_blobs(name_starts_with="observations/hourly/"):
        str_x = x.name
        if(".txt" in str_x):
            list_files.append(x.name)

    for file_name in list_files[:]:    
        try:
            local_file_path =  "./AEMET_FILES_OBSERVATION/"+ file_name
            file_blob_client = blob_service_client.get_blob_client(blob= file_name, container=container_name)
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            with open(local_file_path, "wb") as download_file:
                download_file.write(file_blob_client.download_blob().readall())

        except Exception as e:
            print(e)

def readAemetDataframe():
    import pandas as pd
    import glob
    import datetime
    from pandas.io.json import json_normalize
    from pathlib import Path
    import json
    
    path = './AEMET_FILES_OBSERVATION/observations'
    all_files = glob.glob(path + "/*.txt")

    total_df = pd.DataFrame()

    for filename in Path(path).rglob('*.txt'):
        file_str = ""
        partial_df = pd.DataFrame()
        with open( filename.absolute(), 'rb') as f:
            data = f.readlines()
            data = [json.loads(line) for line in data]
            data = [x for x in data if x is not None]
            partial_df = pd.DataFrame(data)
            total_df = total_df.append(partial_df)
    total_df["MONTH"] = pd.to_numeric(total_df["MONTH"]) + 1
    total_df.columns = [x.replace("\"","") for x in total_df.columns]
    return total_df
    