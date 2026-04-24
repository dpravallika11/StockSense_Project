from functools import wraps
from flask import session, redirect, url_for, flash

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'username' not in session:
            flash("Please log in to access this page.", "error")
            return redirect(url_for('auth.login'))
        return f(*args, **kwargs)
    return decorated_function

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'username' not in session:
            flash("Please log in to access this page.", "error")
            return redirect(url_for('auth.login'))
        if session.get('role') != 'Admin':
            flash("Admin access required.", "error")
            return redirect(url_for('pos.index'))
        return f(*args, **kwargs)
    return decorated_function
