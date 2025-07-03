from flask import (
    Flask,
    request,
    jsonify,
 
)
import pandas as pd
from main import main
from logger_config import logger
from datetime import timedelta
from handler import handler
from flask_cors import CORS
import traceback
from fastapi.responses import JSONResponse

#
app = Flask(__name__)
app.secret_key = "4242"
CORS(
    app, supports_credentials=True, origins=["http://localhost:3000"]
)  # Adjust origin as needed
app.permanent_session_lifetime = timedelta(minutes=30)

# CORS(
#     app, supports_credentials=True, origins=["http://192.168.1.157:8300"]
# )  # Adjust origin as needed
# app.permanent_session_lifetime = timedelta(minutes=30)

# Error handler for 404
@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Resource not found"}), 404


# Error handler for 500
@app.errorhandler(500)
def internal_error(e):
    logger.error(f"500 Error: {str(e)}\n{traceback.format_exc()}")
    return jsonify({"error": "Internal server error"}), 500


# Test endpoint
@app.route("/api/test", methods=["GET"])
def test_endpoint():
    """Test endpoint to verify API is working"""
    return jsonify(
        {
            "status": "success",
            "message": "API is working!",
            "data": {"service": "test", "version": "1.0"},
        }
    )


@app.route("/api/reconciliation", methods=["POST"])
def reconciliation():
    try:
        # # Validate session
        # if "username" not in session:
        #     return jsonify({"error": "Unauthorized"}), 401

        # Validate required fields
        required_fields = ["from_date", "to_date", "service_name"]
        for field in required_fields:
            if field not in request.form:
                return jsonify({"error": f"Missing required field: {field}"}), 400

        # Get form data
        from_date = request.form.get("from_date")
        to_date = request.form.get("to_date")
        service_name = request.form.get("service_name")
        transaction_type = request.form.get("transaction_type", None)
        file = request.files.get("file")

        # Validate file
        if not file or file.filename == "":
            return jsonify({"error": "No file uploaded"}), 400
        print(1)
        # Process data
        result = main(from_date, to_date, service_name, file, transaction_type)
        if isinstance(result, str):
            message = result
            result = ""
            return handler(result, message, service_name)
        else:
            for key, value in result.items():
                # Convert pandas DataFrames to array
                if isinstance(value, pd.DataFrame):
                    # Convert using pandas' built-in NaN handling
                    # print("pd.df loop")
                    value = value.where(pd.notnull(value), None)
                    for col in value.select_dtypes(include=["datetime64[ns]"]).columns:
                        value[col] = (
                            value[col]
                            .astype(object)
                            .where(pd.notnull(value[col]), None)
                        )
                    result[key] = value.to_dict(orient="records")

                elif isinstance(value, list):
                    # Ensure any lists contain proper serializable objects
                    # print("list loop")
                    result[key] = [
                        dict(item) if hasattr(item, "__dict__") else item
                        for item in value
                    ]

                elif hasattr(value, "__dict__"):
                    # Convert objects to dictionaries
                    # ("dict loop")
                    result[key] = value.__dict__
            # print(type(result))
            message = "Data processed successfully!"
            return handler(result, message, service_name)
    except Exception as e:
        logger.error(f"Reconciliation error: {str(e)}\n{traceback.format_exc()}")
        return jsonify({"error": "Failed to process data"}), 500


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
