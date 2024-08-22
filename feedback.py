from database import get_db_connection

class AuthorizationRequestPayload:
    def __init__(self, username, password):
        self.username = username
        self.password = password

class AuthorizationResponse:
    def __init__(self, message, user_id=None, username=None, access_level=None):
        self.message = message
        self.user_id = user_id
        self.username = username
        self.access_level = access_level
def authorize_user(body):
    try:
        connection = get_db_connection()  # Use your existing MySQL connector

        if connection is None:
            raise Exception("Failed to establish database connection.")
        print("Database connection established within Flask!")  # Debugging line

        query = f"""
        SELECT id, access_level FROM user WHERE username = '{body.username}' AND password = '{body.password}'
        """

        with connection.cursor() as cursor:
            cursor.execute(query)
            data = cursor.fetchone()  # Fetch one record
            
            if not data:
                raise Exception("Failed to extract user data")

            user_id, access_level = data

            response = AuthorizationResponse(
                message="Login successful",
                user_id=user_id,
                username=body.username,
                access_level=access_level
            )
            return response

    except Exception as e:
        print(f"Error during authorization: {e}")  # Debugging line
        raise e

    finally:
        if connection:
            connection.close()


