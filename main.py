# main.py
# A simple key-value store using a file as the local database

import pickle
import threading

# The name of the file that stores the key-value pairs
FILE_NAME = "database.pkl"

# A lock object to ensure thread-safety
LOCK = threading.Lock()


# A function to load the key-value pairs from the file
def load_data():
    try:
        with open(FILE_NAME, "rb") as f:
            data = pickle.load(f)
    except FileNotFoundError:
        # If the file does not exist, create an empty dictionary
        data = {}
    return data


# A function to save the key-value pairs to the file
def save_data(data):
    with open(FILE_NAME, "wb") as f:
        pickle.dump(data, f)


# A function to put a key-value pair into the database
def put(key, value):
    # Acquire the lock before accessing the file
    LOCK.acquire()
    try:
        # Load the data from the file
        data = load_data()
        # Update the data with the new key-value pair
        data[key] = value
        # Save the data to the file
        save_data(data)
    finally:
        # Release the lock after accessing the file
        LOCK.release()


# A function to get a value from the database by a key
def get(key):
    # Acquire the lock before accessing the file
    LOCK.acquire()
    try:
        # Load the data from the file
        data = load_data()
        # Return the value associated with the key, or None if not found
        return data.get(key)
    finally:
        # Release the lock after accessing the file
        LOCK.release()


# A function to delete a key-value pair from the database by a key
def delete(key):
    # Acquire the lock before accessing the file
    LOCK.acquire()
    try:
        # Load the data from the file
        data = load_data()
        # Remove the key-value pair from the data, if it exists
        data.pop(key, None)
        # Save the data to the file
        save_data(data)
    finally:
        # Release the lock after accessing the file
        LOCK.release()


# A function to update a value in the database by a key
def update(key, value):
    # Acquire the lock before accessing the file
    LOCK.acquire()
    try:
        # Load the data from the file
        data = load_data()
        # Check if the key exists in the data
        if key in data:
            # Update the value associated with the key
            data[key] = value
            # Save the data to the file
            save_data(data)
        else:
            # Raise an exception if the key does not exist
            raise KeyError(f"Key {key} does not exist in the database")
    finally:
        # Release the lock after accessing the file
        LOCK.release()
