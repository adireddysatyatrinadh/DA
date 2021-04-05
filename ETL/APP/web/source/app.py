import os
import base64
#import File
from flask import Flask, request, abort, jsonify, send_from_directory
from werkzeug.security import generate_password_hash,check_password_hash
import jwt
import datetime
from functools import wraps

app = Flask(__name__)


ETL_INPUT = "/data/DA/ETL/APP/input"
app.config["SECRET_KEY"] = "secretkey"
app.config["username"] = "kims"
app.config["userid"] = "0004"
app.config["password"] = "p@ssw0rd"

# Endpoint to authenticate user using Token.
def validate_token(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        if 'access-token' in request.headers:
            token = request.headers["access-token"]
        if not token:
            return jsonify({'message' : 'Token is missing'})
        try:
            data = jwt.decode(token,app.config["SECRET_KEY"],algorithms="HS256")
            userid = data["userid"]
            token_expiry_dttime= data["token_expiry_dttime"]
            if userid!=app.config["userid"]:
                return jsonify({'message': 'Token userid is invalid'})

        except Exception as e:
            print(e.__str__())
            return jsonify({'message' : e.__str__()})

        user={}
        user["userid"]=app.config["userid"]
        user["username"]=app.config["username"]

        return f(user,*args, **kwargs)
    return decorated

# Endpoint to authenticate user using UserName/Password.
@app.route("/login", methods=["POST"])
def login():
    auth = request.authorization
    print(1)
    if not auth:
        return jsonify({'message': 'authorization required.'})

    if not auth.username:
        return jsonify({'message': 'username required.'})

    if not auth.password:
        return jsonify({'message': 'password required.'})

    if auth.username != app.config["username"]:
        return jsonify({'message': 'username invalid.'})
    print(2)

    print(app.config["password"],"---",auth.password)
    print("auth.password=",auth.password)
    if not check_password_hash(generate_password_hash(app.config["password"]), auth.password):
        return jsonify({'message': 'password invalid.'})

    msg = {}
    msg["userid"] = app.config["userid"]
    msg["token_expiry_dttime"] = str(datetime.datetime.now() + datetime.timedelta(minutes=30))
    token = jwt.encode(msg, app.config["SECRET_KEY"])

    print(jsonify({'access_token': token}))

    return jsonify({'access_token': token})
#  END: login()


# Endpoint to list files on the server.
@app.route("/", methods=["GET"])
def root():
    return "Web Server is Running..."

# Endpoint to list files on the server.
@app.route("/file/list", methods=["POST"])
@validate_token
def list_files():
    return " Welcome "

# Endpoint to write files to the server.
@app.route("/file/write", methods=["POST"])
@validate_token
def write_files(user):
    print(user)
    fdic = request.form.to_dict(flat=False)
    for i in range(len(fdic["file_name"])):
        print("Downloading:%s ...",fdic["file_name"][i])
        file_name=fdic["file_name"][i]
        file_content_b64=fdic["file_content"][i]
        file_content_bin=base64.b64decode(file_content_b64)
        file_path=os.path.join(ETL_INPUT,file_name)
        f = open(file_path, 'w+b')
        f.write(file_content_bin)
        f.close()

    ack={}
    ack["status"]='0'
    ack["errorCd"]=None
    ack["errorMsg"]=None
    return jsonify(ack)

"""
# Endpoint to write files to the server.
@app.route("/file/write", methods=["POST"])
def write_files():
    fdic = request.files.to_dict(flat=False)
    for file_name in fdic.keys():
        print("Downloading:{} ...",file_name)
        file_obj=fdic[file_name][0]
        print(file_name)
        print(file_obj)
        file_path=os.path.join(ETL_INPUT,file_name)
        file_obj.save(file_path)
    ack={}
    ack["status"]='0'
    ack["errorCd"]=None
    ack["errorMsg"]=None
    return jsonify(ack)
"""

if __name__ == "__main__":
    app.run(debug=True, port=5000)
