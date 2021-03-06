import psycopg2.extras
import pandas as pd
import numpy as np
import requests
from getpass import getpass
from zeg.__main__ import main as zeg
import sys
import os
import urllib.parse
import logging

logging.basicConfig(level=os.environ['LOG_LEVEL'])

QUERY_IMAGING_DAY_ZERO = "SELECT measurement_label, min(time_stamp) AS imaging_day_zero " \
    "FROM snapshot " \
    "GROUP BY measurement_label " \
    "ORDER by measurement_label;"

QUERY_DATABASES = "SELECT name FROM ltdbs;"

QUERY_METADATA = "SELECT * " \
                       "FROM metadata_view " \
                       "WHERE id_tag in " \
                       "(SELECT id_tag FROM snapshot WHERE measurement_label = (%s))"

SRC_FILE = 1
SRC_DATABASE = 2

TPA_PLANTDB = os.environ['TPA_PLANTDB_IP']

TPA_WORKSPACE_ID = "HqEiLESn"

DEFAULT_CAMERA_LABEL = "RGB_3D_3D_side_far_0"

user = "readonlyuser"
password = "readonlyuser"


def get_zegami_token():
    zeg_username = os.environ['USERNAME']
    zeg_password = os.environ['PASSWORD']

    data = {
        "username": zeg_username,
        "password": zeg_password
    }

    url = "https://zegami.com/oauth/token/"

    response = requests.post(url, json=data)
    response_data = response.json()
    token = response_data['token']
    return token


def find_or_create_collection(token, db_name, collection_name, project):
    url = "https://zegami.com/api/v0/project/{}/collections/".format(project)
    headers = {'Authorization': 'Bearer {}'.format(token)}
    response = requests.get(url, headers=headers)
    response_data = response.json()

    collection_obj = None
    for i in range(0, len(response_data['collections'])):
        if response_data['collections'][i]['name'] == collection_name:
            logging.debug(response_data['collections'][i])
            collection_obj = response_data['collections'][i]

    if collection_obj is None:
        data = {
            "name": collection_name,
            "description": db_name + " " + collection_name,
            "deepzoom_version": 2
        }

        url = "https://zegami.com/api/v0/project/{}/collections/".format(project)

        headers = {'Content-type': 'application/json', 'Authorization': 'Bearer {}'.format(token)}

        response = requests.post(url, json=data, headers=headers)
        response_data = response.json()
        collection_obj = response_data['collection']

    return collection_obj


def query_database(db_name, query, params=None):

    conn = psycopg2.connect(dbname=db_name, user=user, password=password, host=TPA_PLANTDB)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    logging.debug(cur.mogrify(query, params))

    cur.execute(query, params)

    result = cur.fetchall()
    cur.close()
    conn.close()

    return result


def prepare_database_query(db_name, imaging_day_zero, measurement_label):
    metadata_fields = query_database(db_name, QUERY_METADATA,
                                     [measurement_label, ])
    metadata_fields_df = pd.DataFrame(metadata_fields)
    metadata_fields_df = metadata_fields_df.dropna(how='all', axis=1)
    # TODO: Check metadata exists
    metadata_view_fields = ("metadata_view.\"{}\"," * len(metadata_fields_df.columns)).format(
        *sorted(metadata_fields_df.columns))
    # TODO: Switch based on inputs.
    with open("template-qb.sql", 'r') as query_file:
        query_builder_template = query_file.read()
    query = query_builder_template.format(measurement_label=measurement_label, imaging_day_zero=imaging_day_zero,
                                          metadata_view_fields=metadata_view_fields)
    return query


def upload_dataset_from_database(collection_obj, db_name, query, token, project):
    with open("template-dataset-db.yaml", 'r') as dataset_template_file:
        dataset_template = dataset_template_file.read()
    dataset_yaml = dataset_template.format(database=db_name, query=query.replace("\n", ""), user=user,
                                           password=password, host=TPA_PLANTDB)
    logging.debug(dataset_yaml)
    with open("dataset.yaml", "w") as text_file:
        text_file.write(dataset_yaml)

    with open("dataset-upload.sh", 'r') as dataset_upload_file:
        dataset_upload = dataset_upload_file.read()
    args = dataset_upload.format(dataset_upload_id=collection_obj['upload_dataset_id'], token=token, project=project)
    sys.argv = args.split()
    zeg()


def upload_imageset_from_database(collection_obj, db_name, query, token, project, camera_label):
    url = "https://zegami.com/api/v0/project/{project}/imagesets/{imageset_id}".format(project=project,
                                                                                       imageset_id=collection_obj[
                                                                                           'imageset_id'])
    headers = {'Authorization': 'Bearer {}'.format(token)}
    response = requests.get(url, headers=headers)
    response_data = response.json()

    logging.debug(response_data)

    existing_images = []
    if 'imageset' in response_data:
        if 'images' in response_data['imageset']:
            if response_data['imageset']['images'] is not None:
                existing_images = [i['name'] for i in response_data['imageset']['images'] if i is not None]
    
    logging.debug(existing_images)

    image_path_column = "{}_path".format(camera_label)

    lemnatec_data = query_database(db_name, query)
    lemnatec_df = pd.DataFrame(lemnatec_data)

    if camera_label in lemnatec_df.columns:

        lemnatec_df['image_filenames_only'] = lemnatec_df[image_path_column].str.extract(r'(blob\d+)')
        lemnatec_df = lemnatec_df[~lemnatec_df['image_filenames_only'].isin(existing_images)]
        logging.debug("images that don't yet exist: {}".format(lemnatec_df))
        if len(lemnatec_df[image_path_column].dropna()) > 0:
            
            list_of_paths = "/prod_images/" + db_name + "/" + lemnatec_df[image_path_column].dropna()
            for t_path in list_of_paths:
                 logging.debug(t_path)
                 logging.debug(os.stat(t_path))
                 if os.path.exists(t_path):
                     logging.error('image missing: {}. check fuse mount is mounted correctly.'.format(t_path))

            paths = '\n'.join(["    - {}".format(path) for path in list_of_paths])

            #paths = "    - /prod_images/" + db_name + "/" + lemnatec_df[image_path_column].dropna()
            #paths = paths.str.cat(sep="\n")
            

            logging.debug(paths)
            with open("template-imageset.yaml", 'r') as imageset_template_file:
                imageset_template = imageset_template_file.read()

            upload_imageset(collection_obj, camera_label, imageset_template, paths, project, token)


def upload_dataset_from_file(collection_name, dataset_upload_id, token, project):
    input_file_path = os.path.join("/data_source", collection_name)

    with open("template-dataset-file.yaml", 'r') as dataset_template_file:
        dataset_template = dataset_template_file.read()

    dataset_yaml = dataset_template.format(input_file_path=input_file_path)
    with open("dataset.yaml", "w") as text_file:
        text_file.write(dataset_yaml)

    with open("dataset-upload.sh", 'r') as dataset_upload_file:
        dataset_upload = dataset_upload_file.read()

    args = dataset_upload.format(dataset_upload_id=dataset_upload_id, token=token, project=project)
    sys.argv = args.split()
    zeg()


def upload_imageset_from_file(collection_obj, collection_name, token, project):
    input_filename = collection_name[:-4]
    input_file_path = os.path.join("/data_source", collection_name)

    lemnatec_df = pd.read_csv(input_file_path)

    with open("template-imageset.yaml", 'r') as imageset_template_file:
        imageset_template = imageset_template_file.read()

    camera_label = "RGB SV1"
    image_path_column = "{} image path".format(camera_label)
    if image_path_column not in lemnatec_df.columns:
        image_path_column = "RGB_3D_3D_side_far_0"

    for path in lemnatec_df[image_path_column]:
        t_path = os.path.join("/export_images/plantdb/tpa_backup", input_filename, path)
        logging.debug(t_path)
        logging.debug(os.stat(t_path))

    paths = "    - /export_images/plantdb/tpa_backup/" + input_filename + "/" + lemnatec_df[image_path_column].dropna()
    paths = paths.str.cat(sep="\n")
    upload_imageset(collection_obj, image_path_column, imageset_template, paths, project, token)


def upload_imageset(collection_obj, image_path_column, imageset_template, paths, project, token):
    imageset_yaml = imageset_template.format(paths=paths, path_column=image_path_column,
                                             collection_id=collection_obj['id'],
                                             dataset_id=collection_obj['dataset_id'])
    with open("imageset.yaml", "w") as text_file:
        text_file.write(imageset_yaml)
    logging.debug(imageset_yaml)
    with open("imageset-upload.sh", 'r') as imageset_upload_file:
        imageset_upload = imageset_upload_file.read()
    logging.debug(imageset_upload)
    args = imageset_upload.format(imageset_id=collection_obj['imageset_id'], token=token, project=project)
    sys.argv = args.split()
    zeg()


def fix_datatypes(collection_obj, token, project):
    area_columns = ["Projected Shoot Area",
                    "sideFarprojectedshootarea",  # "Side Far Projected Shoot Area",
                    "Side Lower Projected Shoot Area",
                    "Side Upper Projected Shoot Area",
                    "Top Projected Shoot Area"]

    for column in area_columns:
        url = "https://zegami.com/api/v0/project/{project}/datasets/{dataset_id}/columns/{column_name}/fields".format(
            project=project, dataset_id=collection_obj['dataset_id'], column_name=urllib.parse.quote(column))

        data = {"type": "number", "zegami:schema": {"datatype": "integer", "userDatatype": "integer"}}

        headers = {'Content-type': 'application/json', 'Authorization': 'Bearer {}'.format(token)}

        response = requests.patch(url, json=data, headers=headers)

        logging.debug(response.json())


def main():
    token = get_zegami_token()

    if len(sys.argv) > 1:
        if sys.argv[1] == "auto":

            list_of_workspace_ids = pd.read_csv(os.path.join("/projects/projects.csv"))['project_id']

            prod_databases = query_database("LTSystem", QUERY_DATABASES)

            for workspace_id in list_of_workspace_ids:
                for db_record in prod_databases:
                    db_name = db_record['name']
                    logging.info("{}-{}".format(workspace_id,db_name))

                    df = pd.read_csv(os.path.join("/projects", workspace_id))
                    mls_in_this_workspace_and_db = df.loc[df['database'] == db_name]

                    mls_and_imaging_day_zeros = pd.DataFrame([i.copy() for i in query_database(db_name, QUERY_IMAGING_DAY_ZERO)])

                    if workspace_id == TPA_WORKSPACE_ID:
                        mls_and_id0_in_this_workspace_and_db = pd.merge(mls_in_this_workspace_and_db, mls_and_imaging_day_zeros, how='outer')
                    else:
                        mls_and_id0_in_this_workspace_and_db = pd.merge(mls_in_this_workspace_and_db, mls_and_imaging_day_zeros)

                    logging.debug(mls_and_id0_in_this_workspace_and_db)

                    for i, ml_record in mls_and_id0_in_this_workspace_and_db.iterrows():
                        measurement_label = ml_record['measurement_label']
                        imaging_day_zero = ml_record['imaging_day_zero']
                        camera_label = ml_record['camera_label'] if not pd.isnull(ml_record['camera_label']) else DEFAULT_CAMERA_LABEL

                        logging.info("{}-{}-{}-{}".format(workspace_id, db_name, measurement_label, camera_label))

                        collection_obj = find_or_create_collection(token, db_name, measurement_label, workspace_id)
                        logging.info("collection found or created")

                        query = prepare_database_query(db_name, imaging_day_zero, measurement_label)

                        upload_dataset_from_database(collection_obj, db_name, query, token, workspace_id)
                        logging.info("uploaded data")

                        upload_imageset_from_database(collection_obj, db_name, query, token, workspace_id, camera_label)
                        logging.info("uploaded images")

                        fix_datatypes(collection_obj, token, workspace_id)
    else:
        workspace_id = TPA_WORKSPACE_ID

        data_source = int(input("File [1] or Database[2]?"))
        if data_source == SRC_FILE:
            files = sorted(os.listdir("/data_source"))
            for i, f in enumerate(files):
                print(i, f)
            file_selection = int(input("Select File:"))

            db_name = ""

            collection_name = files[file_selection]

            collection_obj = find_or_create_collection(token, db_name, collection_name, workspace_id)

            upload_dataset_from_file(collection_name, collection_obj['upload_dataset_id'], token, workspace_id)

            upload_imageset_from_file(collection_obj, collection_name, token, workspace_id)
        elif data_source == SRC_DATABASE:
            # TODO: Switch based on inputs between LTSystem and LTSystem_Project or Production
            prod_databases = query_database("LTSystem", QUERY_DATABASES)

            for i, database in enumerate(prod_databases):
                print("{}:\t{}".format(i, database['name']))

            db_selection = int(input("Select Database: "))
            db_name = prod_databases[db_selection]['name']

            mls_and_imaging_day_zeros = query_database(db_name, QUERY_IMAGING_DAY_ZERO)

            for i, measurement_label in enumerate(mls_and_imaging_day_zeros):
                print("{}:\t{}".format(i, measurement_label['measurement_label']))

            ml = int(input("Enter a number: "))

            measurement_label = mls_and_imaging_day_zeros[ml]['measurement_label']
            imaging_day_zero = mls_and_imaging_day_zeros[ml]['imaging_day_zero']

            #TODO: Select camera_label
            camera_label = "RGB_3D_3D_side_far_0"

            collection_obj = find_or_create_collection(token, db_name, measurement_label, workspace_id)

            query = prepare_database_query(db_name, imaging_day_zero, measurement_label)

            upload_dataset_from_database(collection_obj, db_name, query, token, workspace_id)

            upload_imageset_from_database(collection_obj, db_name, query, token, workspace_id, camera_label)

            fix_datatypes(collection_obj, token, workspace_id)

        else:
            logging.error("Invalid data source selection.")
            exit()

main()
