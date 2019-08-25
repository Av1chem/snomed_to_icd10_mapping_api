#!flask/bin/python
from flask import Flask, request, jsonify
import mapping

app = Flask(__name__)

@app.route('/', methods=['POST'])
def do_mapping():
    content = request.json
    result = mapping.perform_mapping(content, app.root_path)
    return jsonify(result)

if __name__ == '__main__':
    app.run(debug=True)