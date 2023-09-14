import random
from datetime import datetime, timedelta

from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["mydatabase"]
collection = db["semi_estructured"]


def populate():
    """'
    Populate the collection with 10,000 documents.
    """

    def random_date():
        start_date = datetime(2019, 1, 1)
        end_date = datetime.now()
        time_delta = end_date - start_date
        random_days = random.randint(0, time_delta.days)
        return start_date + timedelta(days=random_days)

    def random_mixed_type():
        choice = random.choice(["int", "str", "list"])
        return_data = None
        if choice == "int":
            return_data = random.randint(1, 100)
        if choice == "str":
            return_data = random.choice(["apple", "banana", "cherry"])
        if choice == "list":
            return_data = random.sample(range(10), 3)
        return return_data

    def generate_document():
        # Base data
        doc = {
            "name": random.choice(["Alice", "Bob", "Charlie", "David"]),
            "age": random.randint(20, 50),
            "isActive": random.choice([True, False]),
            "score": round(random.uniform(0, 100), 2),
            "createdAt": random_date(),
            "mixedType": random_mixed_type(),
        }

        # Semi-structured data
        if random.choice([True, False]):
            doc["address"] = {
                "street": random.choice(["Main St", "Second Ave", "Third Blvd"]),
                "city": random.choice(["NYC", "LA", "SF"]),
                "zipcode": str(random.randint(10000, 99999)),
            }

        if random.choice([True, False]):
            doc["hobbies"] = random.sample(
                ["reading", "traveling", "cooking", "hiking", "painting"],
                k=random.randint(1, 4),
            )

        if random.choice([True, False]):
            doc["job"] = {
                "title": random.choice(["engineer", "doctor", "teacher", "lawyer"]),
                "yearsExperience": random.randint(1, 10),
                "salary": random.randint(50000, 150000),
            }

        if random.choice([True, False]):
            # Array of arrays field
            matrix_size = random.randint(1, 3)
            doc["dataMatrix"] = [
                [random.randint(0, 9) for _ in range(matrix_size)]
                for _ in range(matrix_size)
            ]

        if random.choice([True, False]):
            # List of dictionaries (projects)
            project_count = random.randint(1, 3)
            doc["projects"] = [
                {
                    "title": random.choice(["Website", "App", "Database", "Game"]),
                    "status": random.choice(["Started", "In Progress", "Completed"]),
                    "completionDate": (
                        datetime.now() + timedelta(days=random.randint(0, 365))
                    ).strftime("%Y-%m-%d"),
                }
                for _ in range(project_count)
            ]

        return doc

    # Populate the collection
    for _ in range(10000):
        document = generate_document()
        collection.insert_one(document)

    print("Data populated!")
