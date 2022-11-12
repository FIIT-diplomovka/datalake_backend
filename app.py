from dotenv import load_dotenv
import os
if os.path.exists("./.env"):
    load_dotenv()
from flask import Flask
from flask_cors import CORS
from routes.writes import write
from routes.reads import read



app = Flask(__name__)
app.register_blueprint(write, name="write")
app.register_blueprint(read, name="read")
cors = CORS(app, resources={r"*": {"origins": "*"}})





if __name__ == "__main__":
    app.run("0.0.0.0")