import json
import logging
import os

from zipfile import ZipFile
import csv

import cosmotech_api
from azure.identity import DefaultAzureCredential
from cosmotech_api.api.dataset_api import DatasetApi
from cosmotech_api.api.runner_api import RunnerApi
from cosmotech_api.api.workspace_api import WorkspaceApi
from rich.logging import RichHandler

LOGGER = logging.getLogger("my_etl_logger")

_format = "[ETL] %(message)s"

logging.basicConfig(
    format=_format,
    datefmt="[%Y/%m/%d-%X]",
    handlers=[RichHandler(rich_tracebacks=True,
                          omit_repeated_times=False,
                          show_path=False,
                          markup=True)])
LOGGER.setLevel(logging.INFO)


def main():
    LOGGER.info("Starting the ETL Run")
    credentials = DefaultAzureCredential()
    scope = os.environ.get("CSM_API_SCOPE")
    access_token = credentials.get_token(scope).token
    configuration = cosmotech_api.Configuration(
        host=os.environ.get("CSM_API_URL"),
        discard_unknown_keys=True,
        access_token=access_token
    )
    parameters = dict()
    with cosmotech_api.ApiClient(configuration) as api_client:
        runner_api_instance = RunnerApi(api_client)
        dataset_api_instance = DatasetApi(api_client)
        workspace_api_instance = WorkspaceApi(api_client)
        runner_data = runner_api_instance.get_runner(organization_id=os.environ.get("CSM_ORGANIZATION_ID"),
                                                     workspace_id=os.environ.get("CSM_WORKSPACE_ID"),
                                                     runner_id=os.environ.get("CSM_RUNNER_ID"))
        LOGGER.info("Loaded run data")
        target_dataset = dataset_api_instance.find_dataset_by_id(
            organization_id=os.environ.get("CSM_ORGANIZATION_ID"),
            dataset_id=runner_data['dataset_list'][0])
        LOGGER.info("Loaded target dataset info")
        # Pre-read of all workspace files to ensure ready to download AZ storage files
        all_api_files = workspace_api_instance.find_all_workspace_files(
            organization_id=os.environ.get("CSM_ORGANIZATION_ID"),
            workspace_id=os.environ.get("CSM_WORKSPACE_ID"))

        # Loop over all parameters
        for parameter in runner_data['parameters_values']:
            value = parameter['value']
            var_type = parameter['var_type']
            param_id = parameter['parameter_id']

            LOGGER.info(f"Found parameter '{param_id}' with value '{value}'")

            # Download "%DATASETID%" files if AZ storage + workspace file based
            if var_type == "%DATASETID%":
                dataset = dataset_api_instance.find_dataset_by_id(
                    organization_id=os.environ.get("CSM_ORGANIZATION_ID"),
                    dataset_id=value)
                param_values = dataset['connector']['parameters_values']

                # Check if file is AZ storage based and is a workspace file
                _base_file_name = param_values.get('AZURE_STORAGE_CONTAINER_BLOB_PREFIX', False)
                if not _base_file_name or "%WORKSPACE_FILE%" not in _base_file_name:
                    LOGGER.warning(f"Parameter '{param_id}' is not a downloadable dataset - Skipping it")
                    continue
                LOGGER.info(f"Parameter '{param_id}' is a downloadable dataset")
                _file_name = _base_file_name.replace('%WORKSPACE_FILE%/', '')
                tmp_dataset_dir = os.environ.get("CSM_PARAMETERS_ABSOLUTE_PATH") + "/" + param_id
                os.makedirs(tmp_dataset_dir, exist_ok=True)
                # Lookup in previously loaded list of files to find all files corresponding to the dataset
                existing_files = list(_f.to_dict().get('file_name')
                                      for _f in all_api_files
                                      if _f.to_dict().get('file_name', '').startswith(_file_name))
                files_len = len(existing_files)
                files_txt, those_txt = ["file", "files"][files_len > 1], ["it", "those"][files_len > 1]
                LOGGER.info(f"  - {len(existing_files)} {files_txt} found in the dataset, downloading {those_txt}")
                # Download and write locally all files corresponding
                for _existing_file_name in existing_files:
                    dl_file = workspace_api_instance.download_workspace_file(
                        file_name=_existing_file_name,
                        organization_id=os.environ.get("CSM_ORGANIZATION_ID"),
                        workspace_id=os.environ.get("CSM_WORKSPACE_ID"))
                    target_file = os.path.join(tmp_dataset_dir, _existing_file_name.split('/')[-1])
                    with open(target_file, "wb") as tmp_file:
                        tmp_file.write(dl_file.read())
                value = tmp_dataset_dir
            parameters[param_id] = (value, var_type)
    # Now all parameters and Datasets are available locally
    # %DATASETID% type files have value replaced with folder containing them

    LOGGER.info("All parameters are loaded")

    bars = list()

    bar = {
        "type": "Bar",
        "name": "MyBar",
        "params": f"""NbWaiters: {int(parameters["num_waiters"][0])},""" +
                  f"""RestockQty: {int(parameters["restock_quantity"][0])},""" +
                  f"""Stock: {int(parameters["stock"][0])}""",
    }
    bars.append(bar)

    base_path = parameters["bar_instance"][0]
    with ZipFile(base_path + "/brewery_instance.zip") as zip:
        zip.extractall(base_path)

    base_path = base_path + "/reference"
    customers = list()
    with open(base_path + "/Nodes/Customer.csv") as _f:
        LOGGER.info("Found 'Customer' list")
        csv_r = csv.DictReader(_f)
        for row in csv_r:
            customer = {
                "type": "Customer",
                "name": row['id'],
                "params": f"Name: '{row['id']}',"
                          f"Satisfaction: {row['Satisfaction']},"
                          f"SurroundingSatisfaction: {row['SurroundingSatisfaction']},"
                          f"Thirsty: {row['Thirsty']}",
            }
            customers.append(customer)

    satisfactions = list()
    with open(base_path + "/Edges/arc_Satisfaction.csv") as _f:
        LOGGER.info("Found 'Customer satisfaction' relation")
        csv_r = csv.DictReader(_f)
        for row in csv_r:
            satisfaction = {
                "type": "satisfaction",
                "source": row['source'],
                "target": row['target'],
                "name": row['name'],
                "params": f"a: 'a'"
            }
            satisfactions.append(satisfaction)

    links = list()
    with open(base_path + "/Edges/Bar_vertex.csv") as _f:
        LOGGER.info("Found 'Bar vertex' relation")
        csv_r = csv.DictReader(_f)
        for row in csv_r:
            link = {
                "type": "bar_vertex",
                "source": row['source'],
                "target": row['target'],
                "name": row['name'],
                "params": f"a: 'a'"
            }
            links.append(link)

    LOGGER.info("Writing data into target Dataset")

    dataset = dataset_api_instance.find_dataset_by_id(os.environ.get("CSM_ORGANIZATION_ID"), runner_data['dataset_list'][0])
    if dataset.status:
        dataset_api_instance.twingraph_query(
            organization_id=os.environ.get("CSM_ORGANIZATION_ID"),
            dataset_id=runner_data['dataset_list'][0],
            dataset_twin_graph_query={"query": "MATCH (n) DETACH DELETE n"})

    create = dataset_api_instance.create_twingraph_entities(
        organization_id=os.environ.get("CSM_ORGANIZATION_ID"),
        dataset_id=runner_data['dataset_list'][0],
        type="node",
        graph_properties=bars + customers)

#    create = json.loads(create.replace("][", ","))
#
#    ids = dict()
#    for item in create:
#        twin_id = item["a"]["id"]
#        entity_id = item["a"]["properties"]["id"]
#        ids[entity_id] = twin_id
#
#    for link in links:
#        link["source"] = ids[link["source"]]
#        link["target"] = ids[link["target"]]

    dataset_api_instance.create_twingraph_entities(
        organization_id=os.environ.get("CSM_ORGANIZATION_ID"),
        dataset_id=runner_data['dataset_list'][0],
        type="relationship",
        graph_properties=satisfactions + links)

    LOGGER.info("ETL Run finished")


if __name__ == "__main__":
    main()
