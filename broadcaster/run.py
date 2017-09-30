#!flask/bin/python
from flask import Flask
from flask import request
import logging
import broadcaster

app = Flask(__name__)

@app.route('/', methods=['POST'])
def index():

    logging.debug("Broadcast request received :: " + str(request))
    request_data = request.get_json()

    broadcast_id = ''
    data = ''
    meta = ''
    for key, value in request_data.items():
        if key == 'meta':
            for key, value in value.items():
                if key == 'broadcast_id':
                    broadcast_id=value
            meta = {key:value}
        elif key == 'data':
            data = value

    broadcaster.main(data, meta, broadcast_id)

    # flags = request.form('flags')
    # broadcast_id = request.args.get('broadcast_id')
    # if data is None or flags is None:
    #     return "ERROR ERROR ERROR - You've got the url right but everything is just.. well it's just wrong."
    # else:
    #     broadcaster.main(data, flags, broadcast_id)
    return('\n'+'Success!'+'\n\n')


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', filename='router.log', level=logging.DEBUG)
    app.run(debug=True)