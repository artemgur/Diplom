import requests


def info():
    return requests.get('http://localhost:8083').text


def connectors_list():
    return requests.get('http://localhost:8083/connectors').text


def connector_info(connector_name):
    return requests.get('http://localhost:8083/connectors/' + connector_name).text


def connector_status(connector_name):
    return requests.get(f'http://localhost:8083/connectors/{connector_name}/status').text


print(connector_status('postgres_debezium_source_connector'))
print(connector_status('postgres_sink_connector'))
