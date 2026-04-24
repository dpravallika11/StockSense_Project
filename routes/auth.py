from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from models.user_model import UserModel
from database import db

auth_bp = Blueprint('auth', __name__)
user_model = UserModel(db)

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        user = user_model.verify_user(username, password)
        if user:
            session['username'] = user['username']
            session['role'] = user['role']
            session['store_name'] = user['store_name']
            
            if user['role'] == 'Admin':
                return redirect(url_for('dashboard.index'))
            else:
                return redirect(url_for('pos.index'))
        else:
            flash("Invalid username or password", "error")
            
    return render_template('login.html')

@auth_bp.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        store_name = request.form.get('store_name')
        username = request.form.get('username')
        password = request.form.get('password')
        role = request.form.get('role', 'Cashier') # Default Cashier
        
        user = user_model.create_user(store_name, username, password, role)
        if user:
            flash("Registration successful. Please login.", "success")
            return redirect(url_for('auth.login'))
        else:
            flash("Username already exists", "error")
            
    return render_template('register.html')

@auth_bp.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('auth.login'))
