import requests


def add_connector(filename):
    with open(filename, 'r') as file:
        json_string = file.read()
    headers = {'Content-Type': 'application/json'}
    response = requests.post('http://localhost:8083/connectors', headers=headers, data=json_string)
    print(response.text)


if __name__ == '__main__':
    add_connector('postgres_debezium_source.json')
    #add_connector('postgres_sink.json')