import psycopg2.extras
import pandas as pd
import requests
from getpass import getpass
from zeg.__main__ import main as zeg
import sys
import os

SRC_FILE = 1
SRC_DATABASE = 2


# File or Database
data_source = int(input("File [1] or Database[2]?"))

#TODO:  Is image source different?



if data_source == SRC_FILE:
    files = os.listdir("/data_source")
    for i, f in enumerate(files):
        print(i, f)
    file_selection = int(input("Select File:"))

    db_name = ""
    collection_name = files[file_selection]
    input_file_path = os.path.join("/data_source",collection_name)

    lemnatec_df = pd.read_csv(input_file_path)
#    print(lemnatec_df[1:10])





elif data_source == SRC_DATABASE:
    TPA_PLANTDB = "192.168.0.24"
    #TPA_PLANTDB = "tpa-plantdb.plantphenomics.org.au"

    user = "readonlyuser"
    password = "readonlyuser"

    # dbname – the database name (database is a deprecated alias)
    # user – user name used to authenticate
    # password – password used to authenticate
    # host – database host address (defaults to UNIX socket if not provided)
    # port – connection port number (defaults to 5432 if not provided)
    conn = psycopg2.connect(dbname="LTSystem", user=user, password=password, host=TPA_PLANTDB)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT name FROM ltdbs;")
    prod_databases = cur.fetchall()
    cur.close()
    conn.close()

    for i, database in enumerate(prod_databases):
        print("{}:\t{}".format(i, database['name']))

    db_selection = int(input("Select Database: "))
    db_name = prod_databases[db_selection]['name']

    conn = psycopg2.connect(dbname=db_name, user=user, password=password, host=TPA_PLANTDB)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT measurement_label, min(time_stamp) AS imaging_day FROM snapshot GROUP BY measurement_label ORDER by measurement_label;")
    measurement_labels = cur.fetchall()
    cur.close()
    conn.close()

    for i, measurement_label in enumerate(measurement_labels):
        print("{}:\t{}".format(i, measurement_label['measurement_label']))

    ml = int(input("Enter a number: "))

    measurement_label = measurement_labels[ml]['measurement_label']
    imaging_day = measurement_labels[ml]['imaging_day']

    conn = psycopg2.connect(dbname=db_name, user=user, password=password, host=TPA_PLANTDB)
    cur = conn.cursor()
    cur.execute("SELECT measurement_label, min(time_stamp) FROM snapshot GROUP BY measurement_label ORDER by measurement_label;")
    measurement_labels = cur.fetchall()
    cur.close()
    conn.close()

    conn = psycopg2.connect(dbname=db_name, user=user, password=password, host=TPA_PLANTDB)
    cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM metadata_view WHERE id_tag in (SELECT id_tag FROM snapshot WHERE measurement_label = (%s))", [measurement_label,])
    metadata_fields = cur.fetchall()
    cur.close()
    conn.close()

    df = pd.DataFrame(metadata_fields)
    df = df.dropna(how='all', axis=1)

    #TODO: Check metadata exists

    metadata_view_fields = ("metadata_view.\"{}\"," * len(df.columns)).format(*sorted(df.columns))

    with open("template-qb.sql", 'r') as query_file:
        query_builder_template = query_file.read()

    query = query_builder_template.format(measurement_label=measurement_label, imaging_day=imaging_day, metadata_view_fields=metadata_view_fields)
    print(query)

    conn = psycopg2.connect(dbname=db_name, user=user, password=password, host=TPA_PLANTDB)
    cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
    cur.execute(query)
    lemnatec_data = cur.fetchall()
    cur.close()
    conn.close()

    collection_name = measurement_label
    lemnatec_df = pd.DataFrame(lemnatec_data)

else:
    print("Invalid data source selection.")



zeg_username = input('Username: ')
zeg_password = getpass()

data = {
    "username": zeg_username,
    "password": zeg_password
}

url = "https://zegami.com/oauth/token/"

response = requests.post(url, json=data)
response_data = response.json()
token = response_data['token']

url = "https://zegami.com/api/v0/project/OVdSdE5n/collections/"
headers = {'Authorization': 'Bearer {}'.format(token)}
response = requests.get(url, headers=headers)
response_data = response.json()

collection_obj = None
for i in range(0, len(response_data['collections'])):
    if response_data['collections'][i]['name'] == collection_name:
        print(response_data['collections'][i],flush=True)
        collection_obj = response_data['collections'][i]

if collection_obj is None:

    data = {
        "name": collection_name,
        "description": db_name + " " + collection_name,
        "deepzoom_version": 2
    }

    url = "https://zegami.com/api/v0/project/OVdSdE5n/collections/"

    headers = {'Content-type': 'application/json', 'Authorization': 'Bearer {}'.format(token)}

    response = requests.post(url, json=data, headers=headers)
    response_data = response.json()
    collection_obj = response_data['collection']
    print(response_data,flush=True)


if data_source == SRC_FILE:
    with open("template-dataset-file.yaml", 'r') as dataset_template_file:
        dataset_template = dataset_template_file.read()

    dataset_yaml = dataset_template.format(input_file_path=input_file_path)

elif data_source == SRC_DATABASE:
    with open("template-dataset-db.yaml", 'r') as dataset_template_file:
        dataset_template = dataset_template_file.read()

    dataset_yaml = dataset_template.format(database=db_name, query=query.replace("\n",""), user=user, password=password, host=TPA_PLANTDB)

with open("dataset.yaml", "w") as text_file:
    text_file.write(dataset_yaml)


with open("template-imageset.yaml", 'r') as imageset_template_file:
    imageset_template = imageset_template_file.read()

#TODO: Choice of camera
if data_source == SRC_FILE:
    camera_label = "RGB SV1"
    for path in lemnatec_df["{} image path".format(camera_label)]:
        t_path = os.path.join("/images/plantdb/tpa_backup", path)
        print(t_path)
        print(os.stat(t_path))

    exit()

    paths = "    - /images/plantdb/tpa_backup/" + lemnatec_df["{} image path".format(camera_label)].dropna()
    paths = paths.str.cat(sep="\n")
    imageset_yaml = imageset_template.format(paths=paths, path_column="{} image path".format(camera_label), collection_id=collection_obj['id'], dataset_id=collection_obj['dataset_id'])
    #print(paths)
    #for path in paths:
    #    os.stat(path)


elif data_source == SRC_DATABASE:
    camera_label = "RGB_3D_3D_side_far_0"
    paths = "    - /images/" + lemnatec_df["{}_path".format(camera_label)].dropna()
    paths = paths.str.cat(sep="\n")
    imageset_yaml = imageset_template.format(paths=paths, path_column=camera_label, collection_id=collection_obj['id'], dataset_id=collection_obj['dataset_id'])

#print(lemnatec_df['RGB SV1 image path'])]
#print(paths)

print(imageset_yaml)

with open("imageset.yaml", "w") as text_file:
    text_file.write(imageset_yaml)

with open("dataset-upload.sh", 'r') as dataset_upload_file:
    dataset_upload = dataset_upload_file.read()

args = dataset_upload.format(dataset_upload_id=collection_obj['upload_dataset_id'], token=token)
sys.argv = args.split()
zeg()

with open("imageset-upload.sh", 'r') as imageset_upload_file:
    imageset_upload = imageset_upload_file.read()

args = imageset_upload.format(imageset_id=collection_obj['imageset_id'], token=token)
sys.argv = args.split()
zeg()
