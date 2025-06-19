
from flask import Flask


app = Flask(__name__)


@app.route('/', methods=['GET'])
def hello_world():
           return 'Hello,world!\n'

@app.route('/<name>', methods=['GET'])
def hello_name(name):
    return f'Hello, {name}!\n'


#the line for your flash application
if __name__== '__main__':
    app.run(debug=True, host='0.0.0.0', port=8052)
