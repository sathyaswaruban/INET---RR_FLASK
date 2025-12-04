from flask import Flask, request, jsonify
import pandas as pd
from main import main
from logger_config import logger
from datetime import timedelta
from handler import handler
from flask_cors import CORS
import traceback
from typing import Dict, Any, Optional
from components.IhubUsercounts import inet_count, ebodetailed_data
import numpy as np
from components.vendorexcel import vendorexcel_reconciliation


app = Flask(__name__)
app.secret_key = "inet_secret_key"


# Configure CORS
CORS(app, supports_credentials=True, origins=["http://localhost:3000"])
# CORS(app, supports_credentials=True, origins=["http://192.168.1.157:8300"])


# Constants
REQUIRED_FIELDS = ["from_date", "to_date", "service_name"]
SUCCESS_MESSAGE = "Data processed successfully..!"
FAILURE_MESSAGE = "Failed to process data...! Please try again."
REQIRED_VENDORLEDGER_FIELDS = ["service_name", "vendor_ledger", "vendor_statement"]


def validate_request(request) -> Optional[Dict[str, Any]]:
    """Validate incoming request and return error response if invalid."""
    # Check required fields
    missing_fields = [field for field in REQUIRED_FIELDS if field not in request.form]
    if missing_fields:
        return {"error": f"Missing required fields: {', '.join(missing_fields)}"}, 400

    # Check file upload
    if request.form["service_name"] != "IRCTC":
        if "file" not in request.files or not request.files["file"].filename:
            return {"error": "No file uploaded"}, 400
    return None


def validate_vendor_request(request) -> Optional[Dict[str, Any]]:
    """Validate incoming vendor ledger request and return error response if invalid."""
    # Check required fields
    missing_fields = [
        field
        for field in REQIRED_VENDORLEDGER_FIELDS
        if field not in request.form and field not in request.files
    ]
    if missing_fields:
        return {"error": f"Missing required fields: {', '.join(missing_fields)}"}, 400
    # Check file uploads
    if (
        "vendor_ledger" not in request.files
        or not request.files["vendor_ledger"].filename
    ):
        return {"error": "No vendor ledger file uploaded"}, 400
    if (
        "vendor_statement" not in request.files
        or not request.files["vendor_statement"].filename
    ):
        return {"error": "No vendor statement file uploaded"}, 400
    return None


def clean_nans(obj):
    """Recursively replace NaN, pd.NA, and 'nan' strings with None."""
    if isinstance(obj, float) and (pd.isna(obj) or np.isnan(obj)):
        return None
    if isinstance(obj, str) and obj.strip().lower() in ("nan", "none", ""):
        return None
    if isinstance(obj, dict):
        return {k: clean_nans(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_nans(v) for v in obj]
    return obj


def process_result(result: Any, service_name: str) -> Dict[str, Any]:
    """Process the result from main() into a serializable format."""
    if isinstance(result, str):
        return handler("", result, service_name)

    if not isinstance(result, dict):
        return handler("", FAILURE_MESSAGE, service_name)

    processed_result = {}
    for key, value in result.items():
        if isinstance(value, pd.DataFrame):
            # Handle DataFrame conversion
            value = value.replace({pd.NA: None, np.nan: None})

            # Handle datetime conversion
            for col in value.select_dtypes(include=["datetime64[ns]"]).columns:
                value[col] = value[col].astype(object).where(value[col].notna(), None)

            # ✅ Round numeric values in specific columns to 3 decimals
            for col in ["AMOUNT", "COMMISSION_AMOUNT", "HUB_AMOUNT"]:
                if col in value.columns:

                    def safe_round(x):
                        if pd.isna(x):
                            return None
                        try:
                            return round(float(x), 3)
                        except (ValueError, TypeError):
                            return 0.000  # non-numeric fallback

                    value[col] = value[col].apply(safe_round)
            # value = value.where(pd.notna(value), None)
            processed_result[key] = clean_nans(value.to_dict(orient="records"))
        elif isinstance(value, list):
            processed_result[key] = [
                item if not hasattr(item, "__dict__") else vars(item) for item in value
            ]
        elif hasattr(value, "__dict__"):
            processed_result[key] = vars(value)
        else:
            processed_result[key] = value

    return handler(processed_result, SUCCESS_MESSAGE, service_name)


def process_vendor_result(result: Any, service_name: str) -> Dict[str, Any]:
    """Process the result from vendorexcel_reconciliation() into a serializable format."""

    # If the result is just a message string
    if isinstance(result, str):
        return handler("", result, service_name)

    # If result is not a dict, treat as failure
    if not isinstance(result, dict):
        return handler("", FAILURE_MESSAGE, service_name)

    processed_result = {}

    for key, value in result.items():
        if isinstance(value, pd.DataFrame):
            # Replace missing values with None
            value = value.replace({pd.NA: None, np.nan: None})

            # Convert datetime columns to YYYY-MM-DD string (no time)
            for col in value.select_dtypes(include=["datetime64[ns]"]).columns:
                value[col] = (
                    value[col].dt.strftime("%Y-%m-%d").where(value[col].notna(), None)
                )

            # Convert dataframe to list of dicts
            processed_result[key] = clean_nans(value.to_dict(orient="records"))

        elif isinstance(value, list):
            # Convert objects in list to dict if needed
            processed_result[key] = [
                item if not hasattr(item, "__dict__") else vars(item) for item in value
            ]

        elif hasattr(value, "__dict__"):
            # Convert custom object to dict
            processed_result[key] = vars(value)

        else:
            # Keep primitive types as-is
            processed_result[key] = value

    return handler(processed_result, SUCCESS_MESSAGE, service_name)


@app.errorhandler(404)
def not_found(e) -> tuple:
    return jsonify({"error": "Resource not found"}), 404


@app.errorhandler(500)
def internal_error(e) -> tuple:
    logger.error(f"500 Error: {str(e)}\n{traceback.format_exc()}")
    return jsonify({"error": "Internal server error"}), 500


@app.route("/api/reconciliation", methods=["POST"])
def reconciliation() -> tuple:
    try:
        # Validate request
        if error_response := validate_request(request):
            return jsonify(error_response[0]), error_response[1]
        request_data = {
            "from_date": request.form["from_date"],
            "to_date": request.form["to_date"],
            "service_name": request.form["service_name"],
            "transaction_type": request.form.get("transaction_type"),
            "file": (
                request.files.get("file") if request.files else None
            ),  # Use .get() to avoid KeyError
        }

        # Process reconciliation
        result = main(**request_data)
        if isinstance(result, str):
            # Original string handling - call handler directly
            return handler("", result, request_data["service_name"])
        else:
            # Original non-string path - process_result then handler
            processed = process_result(result, request_data["service_name"])
            return processed
    except Exception as e:
        logger.error(f"Reconciliation error: {str(e)}\n{traceback.format_exc()}")
        return jsonify(
            handler(None, FAILURE_MESSAGE, request.form.get("service_name", ""))
        )


@app.route("/api/vendorledger_reconciliation", methods=["POST"])
def vendorledger_reconciliation() -> tuple:
    try:
        # Validate request
        if error_response := validate_vendor_request(request):
            return jsonify(error_response[0]), error_response[1]

        # Extract request data
        request_data = {
            "service_name": request.form["service_name"],
            # "transaction_type": request.form.get("transaction_type"),
            "vendor_ledger": request.files["vendor_ledger"],
            "vendor_statement": request.files["vendor_statement"],
        }

        result = vendorexcel_reconciliation(**request_data)
        if isinstance(result, str):
            # Original string handling - call handler directly
            return handler("", result, request_data["service_name"])
        else:
            # Original non-string path - process_result then handler
            processed = process_vendor_result(result, request_data["service_name"])
            return processed
    except Exception as e:
        logger.error(f"Reconciliation error: {str(e)}\n{traceback.format_exc()}")
        return jsonify(
            handler(None, FAILURE_MESSAGE, request.form.get("service_name", ""))
        )


@app.route("/api/getEboData", methods=["GET"])
def get_ebo_data() -> tuple:
    try:
        result = inet_count()
        if isinstance(result, str):
            return handler("", result, "inet_count")
        else:
            processed = process_result(result, "inet_count")
            return processed

    except Exception as e:
        logger.error(f"Error fetching EBO data: {str(e)}\n{traceback.format_exc()}")
        return jsonify(handler(None, FAILURE_MESSAGE, "inet_count"))


@app.route("/api/getEbodetailedData", methods=["POST"])
def get_ebo_detailed_data() -> tuple:
    try:
        # if error_response := validate_request(request):
        #     return jsonify(error_response[0]), error_response[1]

        request_data = {
            "from_date": request.form["from_date"],
            "to_date": request.form["to_date"],
            "tenant_name": request.form["tenantName"],
            "ebo_status": request.form.get("status"),
        }
        result = ebodetailed_data(**request_data)
        if isinstance(result, str):
            return handler("", result, "ebodetailed_data")
        else:
            processed = process_result(result, "ebodetailed_data")
            print(processed)
        return processed
    except Exception as e:
        logger.error(
            f"Error fetching detailed EBO data: {str(e)}\n{traceback.format_exc()}"
        )
        return jsonify(handler(None, FAILURE_MESSAGE, "inet_count"))


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
