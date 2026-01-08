"""
Shared utility functions for the Retail Analytics Dashboard.
"""

import json
from datetime import datetime, date
from typing import Any

# Try to import numpy/pandas for type checking
try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


def make_json_serializable(obj: Any) -> Any:
    """
    Recursively convert an object to be JSON serializable.
    Handles numpy types, pandas types, datetime objects, etc.

    Args:
        obj: Any Python object

    Returns:
        JSON-serializable version of the object
    """
    # Handle None
    if obj is None:
        return None

    # Handle numpy types
    if NUMPY_AVAILABLE:
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, np.bool_):
            return bool(obj)

    # Handle pandas types
    if PANDAS_AVAILABLE:
        if isinstance(obj, pd.Timestamp):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(obj, pd.Timedelta):
            return str(obj)
        # Check for pandas NA/NaN
        try:
            if pd.isna(obj):
                return None
        except (ValueError, TypeError):
            pass

    # Handle datetime types
    if isinstance(obj, datetime):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    if isinstance(obj, date):
        return obj.strftime('%Y-%m-%d')

    # Handle dictionaries recursively
    if isinstance(obj, dict):
        return {str(k): make_json_serializable(v) for k, v in obj.items()}

    # Handle lists/tuples recursively
    if isinstance(obj, (list, tuple)):
        return [make_json_serializable(item) for item in obj]

    # Handle sets
    if isinstance(obj, set):
        return [make_json_serializable(item) for item in obj]

    # Return as-is if it's a basic type
    if isinstance(obj, (str, int, float, bool)):
        return obj

    # Last resort: convert to string
    try:
        return str(obj)
    except Exception:
        return "<non-serializable>"


def safe_json_dumps(obj: Any, indent: int = 2) -> str:
    """
    Safely convert an object to a JSON string.
    Handles non-serializable types gracefully.

    Args:
        obj: Any Python object
        indent: JSON indentation level

    Returns:
        JSON string representation
    """
    try:
        serializable_obj = make_json_serializable(obj)
        return json.dumps(serializable_obj, indent=indent)
    except Exception as e:
        return json.dumps({"error": f"Could not serialize data: {str(e)}"})
