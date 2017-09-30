
import psycopg2
import json
import logging
import requests

def main(data, meta, broadcast_id):

    logging.debug("Broadcast received with broadcast id of: " + broadcast_id)

    #Define our connection string
    conn_string = "host='localhost' port='5432' dbname='test_db' user='rz_broadcaster' password='Br0AdCa5t3r'"
    # logging.debug the connection string we will use to connect
    logging.debug("Connecting to database\n	->%s" % (conn_string))
    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn_string)
    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    cursor = conn.cursor()
    logging.debug("Connected!\n")
    getBroadcasts(cursor, broadcast_id, data, meta)

def getBroadcasts(cursor, broadcast_id, data, meta):
    query = "SELECT broadcast_receiver, trxn_type FROM app.api_broadcaster WHERE broadcast_id = '"+ broadcast_id +"';"
    logging.debug('Query = ' + query)
    cursor.execute(query)
    send_broadcasts(cursor.fetchall(), data, meta)

def send_broadcasts(broadcasts_to_send, data, meta):
    logging.debug('Broadcasts_to_send = ' + str(broadcasts_to_send))
    if broadcasts_to_send is not None:
        for x in broadcasts_to_send:
            send(x, data, meta)
    else:
        logging.debug("No Broadcasts to send matching broadcast id")

def send(data, data_body, meta):
    logging.debug('Trxn_type = '+data[1])
    txn_type = data[1]
    if txn_type == 'api':
        send_api_broadcast(data, data_body, meta)
    # elif txn_type == 'thrift':
    #     send_thrift_broadcast(data, data_body)
    # elif txn_type == 'file':
    #     send_cli_broadcast(data)

# def send_cli_broadcast(data):
#     filename, file_extension = os.path.splitext(data[2])
#     logging.debug('List = ' + str(data[5]))
    # if file_extension == '.py':
    #     logging.debug('Sending data to '+filename+file_extension)
    #     os.system('python ' + data[2] + ' ' + data[4] +' "' + str(data[5])+ '"')
    # elif file_extension == '.jar':
    #     logging.debug('Running Jar File')
    # elif file_extension == '.js':
    #     logging.debug('Running JS file')


def send_api_broadcast(data, data_body, meta):
    logging.debug('api broadcast...')
# TODO allow for posts and gets
    headers = {'Content-Type':'application/json', 'Accept':'*/*'}
    # rebuild json body from request

    dict = {}
    dict["data"] = data_body
    dict["meta"] = meta
    json_body = json.dumps(dict)

    logging.debug('Data_body = '+ str(json_body))
    r = requests.post(data[0], data=json_body, headers=headers) #, auth=('user', 'pass'))
    if 200<=r.status_code<=299:
        return 'something'
    else:
        return 'error' #TODO this should write request and response to db as well

def send_thrift_broadcast(date):
    logging.debug('This does nothing for now...')


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', filename='broadcaster.log', level=logging.DEBUG)
    logging.debug("MAIN METHOD RECIEVED HERE!!")
    main()