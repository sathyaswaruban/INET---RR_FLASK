from flask import jsonify
from logger_config import logger
from typing import Any, Dict


def handler(result: Any, message: str, service_name: str):
    """
    Handles API responses by formatting the result into a standardized JSON structure.
    Logs the response type and status.

    Args:
        result (Any): The result data, can be a string (error) or dict (success).
        message (str): A message describing the result.
        service_name (str): The name of the service responding.

    Returns:
        Response: Flask JSON response with standardized structure.
    """
    if isinstance(result, str):
        logger.info("Error result sent as API")
        logger.info("----------------------------------")
        response = {
            "isSuccess": False,
            "data": result,
            "message": message,
            "service_name": service_name,
        }
        return jsonify(response)
    elif isinstance(result, dict):
        has_data = any(bool(v) for v in result.values())
        logger.info("Result sent as API")
        logger.info("----------------------------------")
        response = {
            "isSuccess": has_data,
            "data": result,
            "message": message,
            "service_name": service_name,
        }
        return jsonify(response)
    else:
        logger.warning("Unexpected result type in handler: %s", type(result))
        response = {
            "isSuccess": False,
            "data": None,
            "message": "Invalid result type.",
            "service_name": service_name,
        }
        return jsonify(response)
