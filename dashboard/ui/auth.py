"""
Authentication utilities for the Retail Analytics Dashboard.
"""

import hashlib
import streamlit as st


def check_password() -> bool:
    """
    Returns True if the user has entered a correct password.

    Uses Streamlit session state to track authentication status.
    Passwords are stored as SHA-256 hashes in Streamlit secrets.
    """

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        # Default passwords if secrets.toml not configured
        default_users = {
            "admin": hashlib.sha256("changeme123".encode()).hexdigest(),
            "analyst": hashlib.sha256("viewonly456".encode()).hexdigest()
        }

        try:
            users = st.secrets["passwords"]
        except Exception:
            users = default_users

        entered_hash = hashlib.sha256(st.session_state["password"].encode()).hexdigest()

        if (st.session_state["username"] in users and
                users[st.session_state["username"]] == entered_hash):
            st.session_state["password_correct"] = True
            st.session_state["logged_in_user"] = st.session_state["username"]
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False

    if st.session_state.get("password_correct", False):
        return True

    # First run or incorrect password
    st.markdown("## Login Required")
    st.text_input("Username", key="username")
    st.text_input("Password", type="password", key="password", on_change=password_entered)

    if "password_correct" in st.session_state and not st.session_state["password_correct"]:
        st.error("Incorrect username or password")

    return False


def logout():
    """Clear session state and logout user."""
    st.session_state.clear()
    st.rerun()


def get_current_user() -> str:
    """Get the currently logged in username."""
    return st.session_state.get("logged_in_user", "Unknown")


def is_admin() -> bool:
    """Check if the current user is an admin."""
    return st.session_state.get("logged_in_user") == "admin"
