import os
from flask import Flask, render_template
from dotenv import load_dotenv
from pymongo import MongoClient
from flask import redirect, url_for, Blueprint

# Load environment variables
load_dotenv()

# Initialize Flask App
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "default-secret-key")

# Initialize MongoDB Connection
from database import db

# Initialize Background Scheduler
from utils.tasks import init_scheduler
init_scheduler(app, db)

# Register Blueprints
from routes.auth import auth_bp
from routes.inventory import inventory_bp
from routes.pos import pos_bp

app.register_blueprint(auth_bp, url_prefix='/auth')
app.register_blueprint(inventory_bp, url_prefix='/inventory')
app.register_blueprint(pos_bp, url_prefix='/pos')

from routes.dashboard import dashboard_bp
app.register_blueprint(dashboard_bp, url_prefix='/dashboard')

# Removed pos placeholder

@app.route('/')
def index():
    return render_template('intro.html')

if __name__ == '__main__':
    app.run(debug=True, port=5000)

