import json
import pandas as pd
from datetime import datetime, date
from decimal import Decimal
from typing import Any


def json_serializer(obj: Any):
    """Safe JSON serializer for pandas, datetime, decimal, etc."""
    if isinstance(obj, (pd.Timestamp, datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Type {type(obj)} not serializable")


def to_json(data, **kwargs) -> str:
    """Wrapper around json.dumps with custom default serializer."""
    return json.dumps(data, default=json_serializer, **kwargs)