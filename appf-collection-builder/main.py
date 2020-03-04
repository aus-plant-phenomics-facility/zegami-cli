import psycopg2.extras
import pandas as pd
import requests
from getpass import getpass
from zeg.__main__ import main as zeg
import sys

TPA_PLANTDB = "192.168.0.24"
#TPA_PLANTDB = "tpa-plantdb.plantphenomics.org.au"

camera_label = "RGB_3D_3D_side_far_0"

user = "readonlyuser"
password = "readonlyuser"

with open("template-qb.sql", 'r') as query_file:
    query_builder_template = query_file.read()

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

query = query_builder_template.format(measurement_label=measurement_label, imaging_day=imaging_day, metadata_view_fields=metadata_view_fields)
print(query)

conn = psycopg2.connect(dbname=db_name, user=user, password=password, host=TPA_PLANTDB)
cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
cur.execute(query)
lemnatec_data = cur.fetchall()
cur.close()
conn.close()

lemnatec_df = pd.DataFrame(lemnatec_data)

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

for i in range(0, len(response_data['collections'])):
    if response_data['collections'][i]['name'] == measurement_label:
        print(response_data['collections'][i])
        collection_obj = response_data['collections'][i]

if not collection_obj:
    data = {
        "name": measurement_label,
        "description": db_name + " " + measurement_label,
        "deepzoom_version": 2

    }

    url = "https://zegami.com/api/v0/project/OVdSdE5n/collections/"

    headers = {'Content-type': 'application/json', 'Authorization': 'Bearer {}'.format(token)}

    response = requests.post(url, json=data, headers=headers)
    response_data = response.json()
    collection_obj = response_data['collection']
    print(response_data)
#



with open("template-dataset.yaml", 'r') as dataset_template_file:
    dataset_template = dataset_template_file.read()

dataset_yaml = dataset_template.format(database=db_name, query=query.replace("\n",""), user=user, password=password, host=TPA_PLANTDB)

with open("dataset.yaml", "w") as text_file:
    text_file.write(dataset_yaml)

with open("template-imageset.yaml", 'r') as imageset_template_file:
    imageset_template = imageset_template_file.read()

paths = "    - /images/" + lemnatec_df["{}_path".format(camera_label)].dropna()
paths = paths.str.cat(sep="\n")
#TODO: Choice of camera
imageset_yaml = imageset_template.format(paths=paths, path_column=camera_label, collection_id=collection_obj['id'], dataset_id=collection_obj['dataset_id'])

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
