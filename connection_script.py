from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
import sys

def connect_to_mongodb():
    """
    Connect to MongoDB database for Project Phase 2
    """
    # Connection settings
    host = "localhost"
    port = 27018
    username = "admin"
    password = "admin"
    
    # Connection URI
    connection_uri = f"mongodb://{username}:{password}@{host}:{port}/?authSource=admin"
    
    try:
        print("=" * 60)
        print("Phase 2: MongoDB Connection Setup")
        print("=" * 60)
        
        # Create a client
        print("\nConnecting to MongoDB...")
        client = MongoClient(
            connection_uri,
            serverSelectionTimeoutMS=5000
        )
        
        # Test the connection
        client.admin.command('ping')
        print("✓ Successfully connected to MongoDB!")
        
        # Get server info
        server_info = client.server_info()
        print(f"✓ MongoDB version: {server_info['version']}")
        
        # List databases
        print("\nAvailable databases:")
        for db_name in client.list_database_names():
            print(f"  - {db_name}")
        
        # Access the hospital NoSQL database
        db = client['hospital_db_nosql']
        print(f"\n✓ Using database: '{db.name}'")
        
        # List collections
        collections = db.list_collection_names()
        if collections:
            print(f"✓ Collections: {collections}")
        else:
            print("  No collections yet - ready for data migration")
        
        print("\n" + "=" * 60)
        print("✓ MongoDB setup complete!")
        print("=" * 60)
        
        return client
        
    except ConnectionFailure:
        print("✗ Failed to connect to MongoDB - Connection refused")
        print("  Make sure MongoDB is running: docker-compose -f docker-compose-mongo.yml up -d")
        sys.exit(1)
    except ServerSelectionTimeoutError:
        print("✗ Failed to connect to MongoDB - Timeout")
        print("  Make sure MongoDB container is running: docker-compose -f docker-compose-mongo.yml up -d")
        sys.exit(1)
    except Exception as e:
        print(f"✗ An error occurred: {e}")
        sys.exit(1)

def show_connection_details():
    """
    Display connection information for documentation
    """
    print("\n" + "=" * 60)
    print("Connection Details:")
    print("=" * 60)
    print("Host: localhost")
    print("Port: 27018")
    print("Username: admin")
    print("Password: admin")
    print("Database: hospital_db_nosql")
    print("=" * 60)

if __name__ == "__main__":
    # Connect to MongoDB
    client = connect_to_mongodb()
    
    # Show connection details
    show_connection_details()
    
    # Close the connection
    client.close()
    print("\n✓ Connection closed successfully")