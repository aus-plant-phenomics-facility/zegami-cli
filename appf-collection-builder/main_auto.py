import psycopg2.extras
import pandas as pd
import requests
from getpass import getpass
from zeg.__main__ import main as zeg
import sys
import os

SRC_FILE = 1
SRC_DATABASE = 2

TPA_PLANTDB = "192.168.0.24"

user = "readonlyuser"
password = "readonlyuser"


# prepare dataset from file


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
            # print(response_data['collections'][i],flush=True)
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
    # dbname – the database name (database is a deprecated alias)
    # user – user name used to authenticate
    # password – password used to authenticate
    # host – database host address (defaults to UNIX socket if not provided)
    # port – connection port number (defaults to 5432 if not provided)
    conn = psycopg2.connect(dbname=db_name, user=user, password=password, host=TPA_PLANTDB)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
#    print(cur.mogrify(query, params))

    cur.execute(query, params)
    
    result = cur.fetchall()
    cur.close()
    conn.close()

    return result


def prepare_database_query(db_name, imaging_day, measurement_label):
    metadata_fields = query_database(db_name, "SELECT * "
                                              "FROM metadata_view "
                                              "WHERE id_tag in "
                                              "(SELECT id_tag FROM snapshot WHERE measurement_label = (%s))",
                                     [measurement_label, ])
    metadata_fields_df = pd.DataFrame(metadata_fields)
    metadata_fields_df = metadata_fields_df.dropna(how='all', axis=1)
    # TODO: Check metadata exists
    metadata_view_fields = ("metadata_view.\"{}\"," * len(metadata_fields_df.columns)).format(
        *sorted(metadata_fields_df.columns))
    with open("template-qb.sql", 'r') as query_file:
        query_builder_template = query_file.read()
    query = query_builder_template.format(measurement_label=measurement_label, imaging_day=imaging_day,
                                          metadata_view_fields=metadata_view_fields)
    return query


def upload_dataset_from_database(collection_obj, db_name, query, token, project):
    with open("template-dataset-db.yaml", 'r') as dataset_template_file:
        dataset_template = dataset_template_file.read()
    dataset_yaml = dataset_template.format(database=db_name, query=query.replace("\n", ""), user=user,
                                           password=password, host=TPA_PLANTDB)
    with open("dataset.yaml", "w") as text_file:
        text_file.write(dataset_yaml)

    with open("dataset-upload.sh", 'r') as dataset_upload_file:
        dataset_upload = dataset_upload_file.read()
    print("'uploading dataset")
    print(collection_obj)
    args = dataset_upload.format(dataset_upload_id=collection_obj['upload_dataset_id'], token=token, project=project)
    print(args)
    sys.argv = args.split()
    zeg()
    print('uploaded datasets')


def upload_imageset_from_database(collection_obj, db_name, query, token, project):
    url = "https://zegami.com/api/v0/project/{project}/imagesets/{imageset_id}".format(project=project,
                                                                                       imageset_id=collection_obj[
                                                                                           'imageset_id'])
    headers = {'Authorization': 'Bearer {}'.format(token)}
    response = requests.get(url, headers=headers)
    response_data = response.json()

    existing_images = []
    if 'imageset' in response_data:
        if 'images' in response_data['imageset']:
            existing_images = [i['name'] for i in response_data['imageset']['images']]
    camera_label = "RGB_3D_3D_side_far_0"
    image_path_column = "{}_path".format(camera_label)

    lemnatec_data = query_database(db_name, query)
    lemnatec_df = pd.DataFrame(lemnatec_data)

    # print(lemnatec_df)

    if camera_label in lemnatec_df.columns:

        lemnatec_df['image_filenames_only'] = lemnatec_df[image_path_column].str.extract(r'(blob\d+)')
        lemnatec_df = lemnatec_df[~lemnatec_df['image_filenames_only'].isin(existing_images)]
        if len(lemnatec_df[image_path_column].dropna()) > 0:
            paths = "    - /prod_images/" + db_name + "/" + lemnatec_df[image_path_column].dropna()
            # print(paths)
            paths = paths.str.cat(sep="\n")

            # print(paths)

            with open("template-imageset.yaml", 'r') as imageset_template_file:
                imageset_template = imageset_template_file.read()
            imageset_yaml = imageset_template.format(paths=paths, path_column=camera_label,
                                                     collection_id=collection_obj['id'],
                                                     dataset_id=collection_obj['dataset_id'])
            with open("imageset.yaml", "w") as text_file:
                text_file.write(imageset_yaml)
            with open("imageset-upload.sh", 'r') as imageset_upload_file:
                imageset_upload = imageset_upload_file.read()
            args = imageset_upload.format(imageset_id=collection_obj['imageset_id'], token=token, project=project)
            sys.argv = args.split()
            zeg()


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
        print(t_path)
        print(os.stat(t_path))

    paths = "    - /export_images/plantdb/tpa_backup/" + input_filename + "/" + lemnatec_df[image_path_column].dropna()
    paths = paths.str.cat(sep="\n")
    imageset_yaml = imageset_template.format(paths=paths, path_column=image_path_column,
                                             collection_id=collection_obj['id'],
                                             dataset_id=collection_obj['dataset_id'])

    with open("imageset.yaml", "w") as text_file:
        text_file.write(imageset_yaml)

    with open("imageset-upload.sh", 'r') as imageset_upload_file:
        imageset_upload = imageset_upload_file.read()

    args = imageset_upload.format(imageset_id=collection_obj['imageset_id'], token=token, project=project)
    sys.argv = args.split()
    zeg()


def main():
    token = get_zegami_token()

    if len(sys.argv) > 1:
        if sys.argv[1] == "auto":

            projects = ["iCFLiDym", "OVdSdE5n"]

            for project in projects:

                prod_databases = query_database("LTSystem", "SELECT name FROM ltdbs;")

                for db_record in prod_databases:
                    db_name = db_record['name']
                    print("{}".format(db_name))


                    if project != "OVdSdE5n":
                        project_mls_df = pd.read_csv(os.path.join("/projects", project))

                        project_mls_this_db = project_mls_df.loc[project_mls_df['database'] == db_name]["measurement_label"]

                        project_mls = tuple(project_mls_this_db.to_list())

#                        print(project_mls.to_list())
#                        print(type(project_mls.to_list()))

                        if len(project_mls) < 1:
                            measurement_labels = []

                        else:
                            measurement_labels = query_database(db_name, """SELECT measurement_label, min(time_stamp) AS imaging_day
                                                            FROM snapshot
                                                            WHERE measurement_label in %s
                                                            GROUP BY measurement_label
                                                            ORDER by measurement_label;""",
                                       (project_mls,))



                    else:
                        measurement_labels = query_database(db_name,
                                                        "SELECT measurement_label, min(time_stamp) AS imaging_day "
                                                        "FROM snapshot "
                                                        "GROUP BY measurement_label "
                                                        "ORDER by measurement_label;")




                    for ml_record in measurement_labels:
                        measurement_label = ml_record['measurement_label']
                        imaging_day = ml_record['imaging_day']
                        print(measurement_label)

                        collection_obj = find_or_create_collection(token, db_name, measurement_label, project)

                        # print(collection_obj)

                        query = prepare_database_query(db_name, imaging_day, measurement_label)

                        upload_dataset_from_database(collection_obj, db_name, query, token, project)

                        print("uploaded data")

                        upload_imageset_from_database(collection_obj, db_name, query, token, project)

                        print("uploaded images")

    else:
        project = "OVdSdE5n"

        data_source = int(input("File [1] or Database[2]?"))
        if data_source == SRC_FILE:
            files = sorted(os.listdir("/data_source"))
            for i, f in enumerate(files):
                print(i, f)
            file_selection = int(input("Select File:"))

            db_name = ""
            collection_name = files[file_selection]

            collection_obj = find_or_create_collection(token, db_name, collection_name, project)

            upload_dataset_from_file(collection_name, collection_obj['upload_dataset_id'], token, project)

            upload_imageset_from_file(collection_obj, collection_name, token, project)
        elif data_source == SRC_DATABASE:
            prod_databases = query_database("LTSystem", "SELECT name FROM ltdbs;")

            for i, database in enumerate(prod_databases):
                print("{}:\t{}".format(i, database['name']))

            db_selection = int(input("Select Database: "))
            db_name = prod_databases[db_selection]['name']

            measurement_labels = query_database(db_name, "SELECT measurement_label, min(time_stamp) AS imaging_day "
                                                         "FROM snapshot "
                                                         "GROUP BY measurement_label "
                                                         "ORDER by measurement_label;")

            for i, measurement_label in enumerate(measurement_labels):
                print("{}:\t{}".format(i, measurement_label['measurement_label']))

            ml = int(input("Enter a number: "))

            measurement_label = measurement_labels[ml]['measurement_label']
            imaging_day = measurement_labels[ml]['imaging_day']

            collection_obj = find_or_create_collection(token, db_name, measurement_label, project)

            query = prepare_database_query(db_name, imaging_day, measurement_label)

            upload_dataset_from_database(collection_obj, db_name, query, token, project)

            upload_imageset_from_database(collection_obj, db_name, query, token, project)

        else:
            print("Invalid data source selection.")
            exit()


main()
