
# app.py
from flask import Flask, request, jsonify
from database import get_db_connection
from typing import List, Dict
import mysql.connector
import requests
from logging.handlers import RotatingFileHandler
import logging

app = Flask(__name__)

# Set up logging
if not app.debug:
    handler = RotatingFileHandler('/home/os/Apps/labs-job/field_force_task_managment/python_wiomlabs_server/flask_app.log', maxBytes=10000, backupCount=1)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    app.logger.addHandler(handler)


# Data classes for structured data
class PartnerLeadsData:
    def __init__(self, user_id: int):
        self.user_id = user_id

class PartnerData:
    def __init__(self, data: List[Dict]):
        self.data = data

def try_get_username(user_id: int) -> str:
    try:
        app.logger.info(f"Fetching username for user_id: {user_id}")

        # Define the SQL query to fetch the username
        query = f"SELECT username FROM wiomlabs.user WHERE id = {user_id}"

        # Establish a database connection
        connection = get_db_connection()
        cursor = connection.cursor()

        # Execute the query
        cursor.execute(query)
        result = cursor.fetchone()

        if result:
            # Extract the username from the result
            username = result[0]
            app.logger.info(f"Username fetched successfully for user_id: {user_id}")
            return username
        else:
            app.logger.warning(f"Username not found for user_id: {user_id}")
            raise ValueError("Username not found for given user ID")

    except mysql.connector.Error as err:
        app.logger.error(f"Database error occurred while fetching username for user_id {user_id}: {err}")
        raise

    finally:
        cursor.close()
        connection.close()
        app.logger.info(f"Database connection closed for user_id: {user_id}")

def get_partner_cops_output_data(body: PartnerLeadsData):
    # Fetch the username using the user_id
    app.logger.info(f"Starting to fetch partner COPS output data for user_id: {body.user_id}")

    username = try_get_username(body.user_id)

    query = f"""
    SELECT * 
    FROM (
        SELECT
            id,
            cx_mobile,
            City_Refined,
            address,
            modified_at,
            zone,
            lat_long,
            final_lead_status,
            Account_Manager,
            cx_name,
            created_at as output_at,
            row_number() over (partition by cx_mobile order by created_at desc) as row_cnt,
            STR_TO_DATE(JSON_UNQUOTE(JSON_EXTRACT(bm_disposition, '$.preferredDate')), '%Y-%m-%d') AS appointment_date
        FROM wiomlabs.bm_cops_disposition
        WHERE JSON_EXTRACT(bm_disposition, '$.customerIntent') = 'Interested'
        AND updated_account_manager = '{username}'
        AND new_status = 'unprocessed'
        AND installation_status = 0
        AND JSON_UNQUOTE(JSON_EXTRACT(bm_disposition, '$.serviceabilityConfidence')) = 'High Confidence (more than 2 points present inside 100 mtrs circle)'
    ) a 
    WHERE row_cnt = 1
    ORDER BY output_at DESC
    """

    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        cursor.execute(query)
        result = cursor.fetchall()

        headers = [desc[0] for desc in cursor.description]
        data = [dict(zip(headers, row)) for row in result]
        app.logger.info(f"Successfully fetched partner COPS output data for user_id: {body.user_id}")

        return PartnerData(data)

    except mysql.connector.Error as err:
        app.logger.error(f"Error fetching partner COPS output data for user_id {body.user_id}: {err}")
        return None

    finally:
        cursor.close()
        connection.close()
        app.logger.info(f"Database connection closed after fetching partner COPS output data for user_id: {body.user_id}")

@app.route('/get_partner_cops_output_data', methods=['POST'])
def handle_get_partner_cops_output_data():
    app.logger.info("Received request to fetch partner COPS output data.")
    body = request.json
    
    try:
        leads_data = PartnerLeadsData(user_id=body['user_id'])
        app.logger.info(f"Parsed user_id: {leads_data.user_id} from request body.")

        partner_data = get_partner_cops_output_data(leads_data)
        
        if partner_data:
            app.logger.info(f"Successfully retrieved partner data for user_id: {leads_data.user_id}")
            return jsonify({"data": partner_data.data}), 200
        else:
            app.logger.warning(f"Failed to retrieve partner data for user_id: {leads_data.user_id}")
            return jsonify({"error": "Failed to retrieve data"}), 500
    
    except Exception as e:
        app.logger.error(f"Exception occurred while handling request: {e}")
        return jsonify({"error": "Internal server error"}), 500



def get_partner_cops_output_follow_up_data(body: PartnerLeadsData):
    user_id = body.user_id
    app.logger.info(f"Starting to fetch partner COPS follow-up data for user_id: {user_id}")

    # Fetch the username
    try:
        username = try_get_username(user_id)
        app.logger.info(f"Username fetched successfully for user_id: {user_id}")
    except Exception as e:
        app.logger.error(f"Failed to fetch username for user_id {user_id}: {e}")
        return {"error": str(e)}

    # Define the SQL query
    query = f"""
    SELECT * 
    FROM
    (
        SELECT 
        p2.id,
        p1.cx_mobile,
        p1.address,
        p1.zone,
        p1.lat_long,
        STR_TO_DATE(JSON_UNQUOTE(JSON_EXTRACT(p1.bm_disposition, '$.preferredDate')), '%Y-%m-%d') AS appointment_date,
        p1.final_lead_status,
        p1.City_Refined,
        p1.Account_Manager,
        p1.modified_at,
        p2.am_disposition,
        p2.am_remark, 
        p2.created_at as output_at, 
        row_number() over (partition by p1.cx_mobile order by p1.created_at desc) as row_cnt
        FROM wiomlabs.bm_cops_disposition p1
        JOIN wiomlabs.partner_cops_output_data_feedback p2
        ON p1.cx_mobile = p2.customer_mobile
        WHERE p2.am_disposition = 'Follow Up with Partner'
        AND p1.Account_Manager = '{username}'
        AND p2.follow_up_status = 'unprocessed'
    ) a 
    WHERE row_cnt = 1
    ORDER BY output_at DESC
    """

    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        cursor.execute(query)
        result = cursor.fetchall()

        headers = [desc[0] for desc in cursor.description]
        data = [dict(zip(headers, row)) for row in result]

        app.logger.info(f"Successfully fetched follow-up data for user_id: {user_id}")

        return PartnerData(data)

    except mysql.connector.Error as err:
        app.logger.error(f"Error fetching follow-up data for user_id {user_id}: {err}")
        return None

    finally:
        cursor.close()
        connection.close()
        app.logger.info(f"Database connection closed after fetching follow-up data for user_id: {user_id}")


@app.route('/get_partner_cops_output_follow_up_data', methods=['POST'])
def handle_get_partner_cops_output_follow_up_data():
    app.logger.info("Received request to fetch partner COPS output follow-up data.")
    body = request.json
    
    try:
        leads_data = PartnerLeadsData(user_id=body['user_id'])
        app.logger.info(f"Parsed user_id: {leads_data.user_id} from request body.")

        partner_data = get_partner_cops_output_follow_up_data(leads_data)
        
        if partner_data:
            app.logger.info(f"Successfully retrieved partner follow-up data for user_id: {leads_data.user_id}")
            return jsonify({"data": partner_data.data}), 200
        else:
            app.logger.warning(f"Failed to retrieve partner follow-up data for user_id: {leads_data.user_id}")
            return jsonify({"error": "Failed to retrieve data"}), 500
    
    except Exception as e:
        app.logger.error(f"Exception occurred while handling follow-up data request: {e}")
        return jsonify({"error": "Internal server error"}), 500


def submit_partner_cops_output_data(body):
    try:
        app.logger.info("Starting submission of partner COPS output data.")

        # Extracting required fields directly from the request body
        customer_mobile = body['customer_mobile'].replace("'", "''")
        am_disposition = body['am_disposition'].replace("'", "''")
        am_closure_reason = body['am_closure_reason'].replace("'", "''")
        am_remark = body['am_remark'].replace("'", "''")
        am_other_text = body['am_other_text'].replace("'", "''")
        am_name = body['am_name'].replace("'", "''")

        app.logger.info("Extracted and sanitized data from the request body.")

        id = body.get('id')
        if not id:
            app.logger.warning("Missing id in the payload.")
            return {"error": "Missing id in the payload"}, 400

        # Define the insert query
        insert_query = f"""
        INSERT INTO partner_cops_output_data_feedback 
        (user_id, customer_mobile, am_disposition, am_closure_reason, am_remark, am_other_text, updated_lat_long, am_name) 
        VALUES ({body['user_id']}, '{customer_mobile}', '{am_disposition}', '{am_closure_reason}', '{am_remark}', '{am_other_text}', '{body['updated_lat_long']}', '{am_name}')
        """

        # Define the update query
        update_query = f"""
        UPDATE bm_cops_disposition 
        SET new_status = 'processed' 
        WHERE id = {id}
        """

        # Execute the queries
        connection = get_db_connection()
        cursor = connection.cursor()

        cursor.execute(insert_query)
        app.logger.info(f"Insert query executed successfully for user_id: {body['user_id']}")

        cursor.execute(update_query)
        connection.commit()
        app.logger.info(f"Update query executed and committed successfully for id: {id}")

        return {"message": "Feedback submitted successfully"}, 200

    except mysql.connector.Error as err:
        app.logger.error(f"Database Execution Error: {err}")
        return {"error": str(err)}, 500

    finally:
        cursor.close()
        connection.close()
        app.logger.info("Database connection closed after submitting partner COPS output data.")



@app.route('/submit_partner_cops_output_data', methods=['POST'])
def handle_submit_partner_cops_output_data():
    app.logger.info("Received request to submit partner COPS output data.")
    body = request.json
    
    try:
        response, status = submit_partner_cops_output_data(body)
        app.logger.info(f"Submission process completed with status: {status}")
        
        return jsonify(response), status
    
    except Exception as e:
        app.logger.error(f"Exception occurred while submitting partner COPS output data: {e}")
        return jsonify({"error": "Internal server error"}), 500


def submit_partner_cops_output_follow_up_data(body):
    try:
        app.logger.info("Starting submission of partner COPS output follow-up data.")

        # Extracting required fields directly from the request body
        customer_mobile = body['customer_mobile'].replace("'", "''")
        am_disposition = body['am_disposition'].replace("'", "''")
        am_closure_reason = body['am_closure_reason'].replace("'", "''")
        am_remark = body['am_remark'].replace("'", "''")
        am_other_text = body['am_other_text'].replace("'", "''")
        am_name = body['am_name'].replace("'", "''")

        app.logger.info("Extracted and sanitized data from the request body.")

        id = body.get('id')
        if not id:
            app.logger.warning("Missing id in the payload.")
            return {"error": "Missing id in the payload"}, 400

        # Define the insert query
        insert_query = f"""
        INSERT INTO partner_cops_output_data_feedback 
        (user_id, customer_mobile, am_disposition, am_closure_reason, am_remark, am_other_text, updated_lat_long, am_name) 
        VALUES ({body['user_id']}, '{customer_mobile}', '{am_disposition}', '{am_closure_reason}', '{am_remark}', '{am_other_text}', '{body['updated_lat_long']}', '{am_name}')
        """

        # Execute the insert query
        connection = get_db_connection()
        cursor = connection.cursor()

        cursor.execute(insert_query)
        app.logger.info(f"Insert query executed successfully for user_id: {body['user_id']}")

        # Define the update query
        update_query = f"""
        UPDATE partner_cops_output_data_feedback 
        SET follow_up_status = 'processed' 
        WHERE id = {id}
        """

        # Execute the update query
        cursor.execute(update_query)
        connection.commit()
        app.logger.info(f"Update query executed and committed successfully for id: {id}")

        return {"message": "Feedback submitted and follow-up status updated successfully"}, 200

    except mysql.connector.Error as err:
        app.logger.error(f"Database Execution Error: {err}")
        return {"error": str(err)}, 500

    finally:
        cursor.close()
        connection.close()
        app.logger.info("Database connection closed after submitting partner COPS output follow-up data.")


@app.route('/submit_partner_cops_output_follow_up_data', methods=['POST'])
def handle_submit_partner_cops_output_follow_up_data():
    app.logger.info("Received request to submit partner COPS output follow-up data.")
    body = request.json
    
    try:
        response, status = submit_partner_cops_output_follow_up_data(body)
        app.logger.info(f"Follow-up submission process completed with status: {status}")
        
        return jsonify(response), status
    
    except Exception as e:
        app.logger.error(f"Exception occurred while submitting partner COPS output follow-up data: {e}")
        return jsonify({"error": "Internal server error"}), 500


class AppVersionInfo:
    def __init__(self, last_modified, url):
        self.last_modified = last_modified
        self.url = url

def am_booking_management_handle_app_latest_version():
    # URL to the GCP bucket where the APK is stored
    file_url = "https://storage.googleapis.com/rgw-public/bm_app/lab_web/app-release.apk"

    try:
        # Make an HTTP HEAD request to fetch the file's metadata
        response = requests.head(file_url)
        response.raise_for_status()

        # Extract the last modified date from the response headers
        last_modified = response.headers.get("Last-Modified")
        if not last_modified:
            return {"error": "Missing Last-Modified header"}, 400

        # Create the version information
        version_info = AppVersionInfo(last_modified=last_modified, url=file_url)

        # Return the version information as a dictionary
        return {"last_modified": version_info.last_modified, "url": version_info.url}, 200

    except requests.exceptions.RequestException as e:
        return {"error": str(e)}, 500

@app.route('/am_booking_management_handle_app_latest_version', methods=['GET'])
def handle_am_booking_management_handle_app_latest_version():
    response, status = am_booking_management_handle_app_latest_version()
    return jsonify(response), status


def verify_user(username, password):
    app.logger.info(f"Attempting to verify user: {username}")
    
    connection = get_db_connection()
    if connection is None:
        app.logger.error("Failed to establish database connection.")
        raise Exception("Failed to establish database connection.")

    query = f"""
    SELECT id, access_level FROM user WHERE username = '{username}' AND password = '{password}'
    """

    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            data = cursor.fetchone()
            
            if not data:
                app.logger.warning(f"User verification failed for username: {username}")
                return None

            user_id, access_level = data
            app.logger.info(f"User verified successfully: {username} with access level: {access_level}")
            return {"user_id": user_id, "username": username, "access_level": access_level}

    finally:
        connection.close()
        app.logger.info(f"Database connection closed after verifying user: {username}")


@app.route('/login', methods=['POST'])
def login():
    app.logger.info("Received login request.")
    body = request.json
    app.logger.info(f"Login request body: {body}")
    
    username = body.get('username')
    password = body.get('password')

    if not username or not password:
        app.logger.warning("Username or password missing in the login request.")
        return jsonify({"message": "Username and password are required"}), 400

    user_data = verify_user(username, password)
    
    if user_data:
        app.logger.info(f"Login successful for user: {username}")
        return jsonify({
            "message": "Login successful",
            "user_id": user_data['user_id'],
            "username": user_data['username'],
            "access_level": user_data['access_level']
        }), 200
    else:
        app.logger.warning(f"Login failed for user: {username}")
        return jsonify({"message": "Invalid username or password"}), 401


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)


