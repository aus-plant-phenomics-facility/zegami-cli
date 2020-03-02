import http

import psycopg2
import psycopg2.extras
import pandas as pd
import subprocess
import requests
from getpass import getpass


TPA_PLANTDB = "192.168.0.24"
#TPA_PLANTDB = "tpa-plantdb.plantphenomics.org.au" #"192.168.0.24"

camera_label = "RGB_3D_3D_side_far_0"


with open("template-qb.sql", 'r') as query_file:
    query_builder_template = query_file.read()

    # dbname – the database name (database is a deprecated alias)
    # user – user name used to authenticate
    # password – password used to authenticate
    # host – database host address (defaults to UNIX socket if not provided)
    # port – connection port number (defaults to 5432 if not provided)
    conn = psycopg2.connect(dbname="LTSystem", user="readonlyuser", password="readonlyuser", host=TPA_PLANTDB)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT name FROM ltdbs;")
    prod_databases = cur.fetchall()
    cur.close()
    conn.close()

    for i, database in enumerate(prod_databases):
        print("{}:\t{}".format(i, database['name']))

    db_selection = int(input("Select Database: "))
    db_name = prod_databases[db_selection]['name']

    conn = psycopg2.connect(dbname=db_name, user="readonlyuser", password="readonlyuser", host=TPA_PLANTDB)
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

    conn = psycopg2.connect(dbname=db_name, user="readonlyuser", password="readonlyuser", host=TPA_PLANTDB)
    cur = conn.cursor()
    cur.execute("SELECT measurement_label, min(time_stamp) FROM snapshot GROUP BY measurement_label ORDER by measurement_label;")
    measurement_labels = cur.fetchall()
    cur.close()
    conn.close()

    conn = psycopg2.connect(dbname=db_name, user="readonlyuser", password="readonlyuser", host=TPA_PLANTDB)
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

    conn = psycopg2.connect(dbname=db_name, user="readonlyuser", password="readonlyuser", host=TPA_PLANTDB)
    cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
    cur.execute(query)
    lemnatec_data = cur.fetchall()
    cur.close()
    conn.close()

    lemnatec_df = pd.DataFrame(lemnatec_data)

    #- / images / 2020 - 02 - 26 / blob446856

#    print(paths)

    #print("- {}".format("/images/",lemnatec_df['camera_label']))

#    exit()

    # TODO: Login
    # touch .auth
    # docker run -it -v $PWD/.auth:/root/.local/share/zegami-cli/.auth zegami-cli zeg login

    # with open("/tmp/output.log", "a") as output:
    #     subprocess.call("docker run --rm wappalyzer/cli https://wappalyzer.com", shell=True, stdout=output,
    #                     stderr=output)



    username = input('Username: ')
    password = getpass()

    data = {
        "username": username,
        "password": password
    }

    url = "https://zegami.com/oauth/token/"

    response = requests.post(url, json=data)
    response_data = response.json()
    token = response_data['token']



    # TODO: Create Collection (HTTP Post)

    data = {
        "name": measurement_label,
        "description": db_name + " " + measurement_label,
        "deepzoom_version": 2

    }

    url = "https://zegami.com/api/v0/project/OVdSdE5n/collections/"

    headers = {'Content-type': 'application/json', 'Authorization': 'Bearer {}'.format(token)}

    response = requests.post(url, json=data, headers=headers)
    response_data = response.json()
    print(response_data)

    # {'collection': {'augment_imageset_id': '5e5c83239e4515000194e1ce', 'created_at': '2020-03-02T03:53:07+00:00',
    #                 'dataset_id': '5e5c83239e4515000194e1cb', 'description': '0000_Production_N 0500 Maize rerun',
    #                 'dynamic': False, 'dz_imageset_id': '5e5c83239e4515000194e1d0',
    #                 'dz_json_dataset_id': '5e5c83239e4515000194e1d2', 'id': '5e5c83239e4515000194e1d3',
    #                 'imageset_dataset_join_id': '5e5c83239e4515000194e1d1', 'imageset_id': '5e5c83239e4515000194e1cd',
    #                 'join_dataset_id': None, 'name': '0500 Maize rerun',
    #                 'scaled_imageset_id': '5e5c83239e4515000194e1cf',
    #                 'status': {'progress': 0, 'status': 'no_data_uploaded'},
    #                 'tag_dataset_id': '5e5c83239e4515000194e1cc', 'upload_dataset_id': '5e5c83239e4515000194e1ca'}}

    # TODO: Dataset YAML template
    with open("template-dataset.yaml", 'r') as dataset_template_file:
        dataset_template = dataset_template_file.read()

        dataset_yaml = dataset_template.format(database=db_name, query=query.replace("\n",""))

        with open("dataset.yaml", "w") as text_file:
            text_file.write(dataset_yaml)

        #print(dataset_yaml)

    # TODO: Imageset YAML template

    #     {paths}
    # dataset_column: {path_column}
    # collection_id: {collection_id}
    # dataset_id: {datasetid}
    with open("template-imageset.yaml", 'r') as imageset_template_file:
        imageset_template = imageset_template_file.read()

        paths = "    - /images/" + lemnatec_df[camera_label].dropna()
        paths = paths.str.cat(sep="\n")
        #TODO: Choice of camera
        imageset_yaml = imageset_template.format(paths=paths, path_column=camera_label, collection_id=response_data['collection']['id'], dataset_id=response_data['collection']['dataset_id'])

        with open("imageset.yaml", "w") as text_file:
            text_file.write(imageset_yaml)

    # TODO: Update dataset
    with open("dataset-upload.sh", 'r') as dataset_upload_file:
        dataset_upload = dataset_upload_file.read()

        command = dataset_upload.format(dataset_upload_id=response_data['collection']['upload_dataset_id'], token=token)

        subprocess.check_call(command.split())

    # TODO: Update imageset
    with open("imageset-upload.sh", 'r') as imageset_upload_file:
        imageset_upload = imageset_upload_file.read()

        command = imageset_upload.format(imageset_id=response_data['collection']['imageset_id'], token=token)

        subprocess.check_call(command.split())
