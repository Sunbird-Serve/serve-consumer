# consumer.py

from fastapi import FastAPI
import pika
import json
import threading
import requests
import ast
import re
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, auth
import time
import threading
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
last_known_data = []
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:8000"],  # Allow Jupiter app
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods (GET, POST, etc.)
    allow_headers=["*"],  # Allow all headers
)

cred = credentials.Certificate("C:\\provider_app\\serve-sandbox-firebase-adminsdk-4i44o-f4802f37ca.json")
firebase_admin.initialize_app(cred)

def create_firebase_user(email, password="evl123"):
    """Create a user in Firebase with the provided email and a default password."""
    try:
        user = auth.create_user(email=email, password=password)
        print(f"Firebase user created successfully: {user.uid}")
        return user.uid
    except Exception as e:
        print(f"Error creating Firebase user: {e}")
        return None

# Function to process volunteer data
def process_volunteer_data(volunteer_data: dict):
    print("Received volunteer data:", volunteer_data)

    firebase_uid = create_firebase_user(volunteer_data.get("email"))
    if firebase_uid:
        print(f"Firebase UID for user: {firebase_uid}")
    else:
        print("Failed to create Firebase user.")
        return

    # Structure the JSON data for user creation
    structured_data_user = {
        "role": ["Volunteer"],
        "agencyId": "ev001",
        "contactDetails": {
            "address": {
                "state": volunteer_data.get("state") or "Unknown",
                "city": volunteer_data.get("city") or "Unknown",
                "country": volunteer_data.get("country") or "Unknown"
            },
            "mobile": volunteer_data.get("phone") or "0000000000",
            "email": volunteer_data.get("email")
        },
        "identityDetails": {
            "gender": (volunteer_data.get("gender") or "Male").capitalize(),
            "dob": volunteer_data.get("dob") or "2000-01-01",
            "name": volunteer_data.get("first_name") or "Unknown",
            "fullname": f"{volunteer_data.get('first_name', 'Unknown')} {volunteer_data.get('last_name', '')}".strip(),
            "Nationality": "Indian"
        },
        "status": "Registered"
    }

    # Log the structured JSON data for user
    print("Structured User Data =", json.dumps(structured_data_user, indent=2))

    # Send the structured user data to the Serve API
    try:
        response_user = requests.post(
            "https://serve-v1.evean.net/api/v1/serve-volunteering/user/",
            headers={"Content-Type": "application/json"},
            json=structured_data_user
        )
        print("Response Status Code:", response_user.status_code)
        print("Response JSON Data:", response_user.json())  # Print JSON if the response has a JSON body
        print("Response Text:", response_user.text)         # Print full text in case the body isn't JSON

        if response_user.status_code == 200:
            print("User successfully created in Serve application.")
            user_id = response_user.json()["result"]["Users"]['osid']  # Capture the returned user ID for profile creation
            raw_language_data = volunteer_data.get("languages_known", "[]")
            language_list = []
            current_date = datetime.now().strftime("%Y-%m-%d")
            if isinstance(raw_language_data, str):
                try:
                    # Check if it's a single language without a JSON-like structure
                    if raw_language_data.isalpha():  # e.g., "English"
                        language_list = [raw_language_data]
                    else:
                        # Replace only standalone u' patterns, preserving the rest of the word
                        cleaned_language_data = re.sub(r"\bu'", "'", raw_language_data)
                        parsed_language_data = ast.literal_eval(cleaned_language_data)
                        
                        # Ensure parsed data is a list of dictionaries
                        if isinstance(parsed_language_data, list) and all(isinstance(lang, dict) for lang in parsed_language_data):
                            language_list = [lang_data["lang"] for lang_data in parsed_language_data if "lang" in lang_data]
                except (ValueError, SyntaxError) as e:
                    print(f"Failed to parse languages_known: {e}")
            else:
                # If it's already a list of dictionaries, extract the language names directly
                language_list = [lang_data["lang"] for lang_data in raw_language_data if isinstance(lang_data, dict) and "lang" in lang_data]
            day_preferred = volunteer_data.get("pref_days", "Thu;Mon")
            time_preferred = volunteer_data.get("pref_slots", "14:45-16:45;14:45-16:45")
            # Structure the JSON data for user profile
            structured_data_profile = {
                "userId": user_id,
                "genericDetails": {
                    "qualification": "Graduate",
                    "affiliation": "Others",
                    "employmentStatus": "Others",
                    "yearsOfExperience": "0"
                },
                "userPreference": {
                    "language": [],
                    "dayPreferred": [],
                    "timePreferred": [],
                    "interestArea": [],
                },
                "skills": [
                    {
                        "skillName": "",  # Replace or leave as example if no skill data
                        "skillLevel": ""
                    }
                ],
                "consentDetails": {
                    "consentGiven": True,
                    "consentDate": current_date,
                    "consentDescription": "User consented to data use"
                },
                "onboardDetails": {
                    "onboardStatus": [
                        {
                            "onboardStep": "Discussion",
                            "status": "Completed"
                        }
                    ],
                    "refreshPeriod": "2 Years",
                    "profileCompletion": "50%"
                },
                "referenceChannelId": "eVidyaloka",
                "volunteeringHours": {
                    "totalHours": 0,
                    "hoursPerWeek": 0
                }
            }

            # Log the structured JSON data for user profile
            print("Structured User Profile Data =", json.dumps(structured_data_profile, indent=2))

            # Send the structured user profile data to the Serve API
            response_profile = requests.post(
                "https://serve-v1.evean.net/api/v1/serve-volunteering/user/user-profile",
                headers={"Content-Type": "application/json"},
                json=structured_data_profile
            )
            
            if response_profile.status_code == 200:
                print("User profile successfully created in Serve application.")
            else:
                print("Failed to create user profile in Serve application:", response_profile.status_code, response_profile.text)

        else:
            print("Failed to create user in Serve application:", response_user.status_code, response_user.text)

    except Exception as e:
        print("An error occurred while creating the user or user profile in Serve application:", str(e))

# Function to listen to the RabbitMQ queue
def start_rabbitmq_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    
    # Declare the queue (should match the provider's queue)
    channel.queue_declare(queue='volunteer_data_queue', durable=True)
    
    # Define the callback for processing messages
    def callback(ch, method, properties, body):
        volunteer_data = json.loads(body)  # Decode JSON message
        process_volunteer_data(volunteer_data)  # Call the processing function
        ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge the message as processed
    
    # Start consuming messages from the queue
    channel.basic_consume(queue='volunteer_data_queue', on_message_callback=callback)
    channel.start_consuming()

# Run RabbitMQ consumer in a separate thread
@app.on_event("startup")
def startup_event():
    threading.Thread(target=start_rabbitmq_consumer, daemon=True).start()

@app.get("/")
async def read_root():
    return {"message": "Consumer application is running and listening for volunteer data"}

@app.post("/trigger-serve-fetch")
async def trigger_serve_fetch():
    try:
        print('In Trigger method')
        structured_data = fetch_and_structure_serve_data()  # Fetch and structure data
        if structured_data:
            return JSONResponse({"message": "Serve data fetch successful.", "data": structured_data}, status_code=200)
        else:
            return JSONResponse({"message": "Error fetching or structuring data."}, status_code=500)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


def fetch_and_structure_serve_data():
    global last_known_data
    try:
        # Fetch data from Serve API
        response = requests.get(
            "https://serve-v1.evean.net/api/v1/serve-need/need/?page=0&status=Nominated"
        )
        response.raise_for_status()
        serve_data = response.json().get("content", [])
        print('Serve Data = ', serve_data)

        # Process and structure data
        structured_data = []
        for item in serve_data:
            structured_item = {
                "need": {
                    "description": item["need"]["description"].replace("\u003Cp\u003E", "").replace("\u003C/p\u003E", ""),
                    "name": item["need"]["name"],
                    "status": item["need"]["status"]
                },
                "occurrence": {
                    "start_date": item["occurrence"]["startDate"],
                    "end_date": item["occurrence"]["endDate"],
                    "day": item["occurrence"]["days"],
                    "frequency": item["occurrence"]["frequency"]
                },
                "time_slot": [
                    {
                        "start_time": time_slot["startTime"],
                        "end_time": time_slot["endTime"],
                        "day": time_slot["day"]
                    } for time_slot in item["timeSlots"]
                ],
                "entity": {
                    "name": item["entity"]["name"],
                    "mobile": item["entity"]["mobile"],
                    "address_line1": item["entity"]["address_line1"],
                    "district": item["entity"]["district"],
                    "state": item["entity"]["state"],
                    "pincode": item["entity"]["pincode"],
                    "category": item["entity"]["category"],
                    "status": item["entity"]["status"]
                },
                "need_type": {
                    "name": item["needType"]["name"],
                    "status": item["needType"]["status"]
                }
            }
            structured_data.append(structured_item)

        # Compare the new data with the last known data to detect changes
        if structured_data != last_known_data:
            print("New data detected; sending to RabbitMQ and Jupiter")
            # send_to_rabbitmq(structured_data)  # Send data to RabbitMQ as needed
            send_to_jupiter_api(structured_data)  # Send structured data to Jupiter endpoint
            last_known_data = structured_data  # Update the last known data
        return structured_data
    except Exception as e:
        print(f"Error fetching or structuring data: {e}")


def send_to_jupiter_api(data):
    """Send structured data to Jupiter's receive_serve_data API endpoint."""
    try:
        for item in data:
            response = requests.post(
                "http://127.0.0.1:8000/api/receive-serve-data/",  # Replace with the actual Jupiter API URL
                json=item,
                headers={"Content-Type": "application/json"}
            )
            if response.status_code == 201:
                print("Data sent to Jupiter successfully.")
            else:
                print("Failed to send data to Jupiter:", response.status_code, response.text)
    except Exception as e:
        print(f"Error sending data to Jupiter: {e}")

# Function to send structured data to RabbitMQ
def send_to_rabbitmq(data):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='serve_data_queue', durable=True)

        # Publish each item in structured data
        for item in data:
            channel.basic_publish(
                exchange='',
                routing_key='serve_data_queue',
                body=json.dumps(item),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            print("Data sent to RabbitMQ:", item)
            

        connection.close()
    except Exception as e:
        print(f"Error sending data to RabbitMQ: {e}")

# Scheduled task to check for updates periodically (e.g., every 5 minutes)
def periodic_data_check(interval=300):
    while True:
        fetch_and_structure_serve_data()
        time.sleep(interval)

# Start the periodic check in a background thread on app startup
@app.on_event("startup")
def startup_event():
    threading.Thread(target=periodic_data_check, args=(300,), daemon=True).start()

def send_data_to_jupiter(structured_data):
    try:
        jupiter_url = "http://127.0.0.1:8000/api/receive-serve-data/"  # Replace with the actual domain or IP of Jupiter
        headers = {"Content-Type": "application/json"}
        
        response = requests.post(jupiter_url, headers=headers, json=structured_data)
        
        if response.status_code == 201:
            print("Data successfully sent to Jupiter.")
        else:
            print(f"Failed to send data to Jupiter: {response.status_code} - {response.text}")

    except Exception as e:
        print(f"Error sending data to Jupiter: {e}")

