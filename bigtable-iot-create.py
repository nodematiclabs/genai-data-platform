import datetime
import random
import time
from google.cloud import bigtable
from google.cloud.bigtable import column_family

# Replace with your project ID
project_id = os.getenv("GOOGLE_PROJECT")

# Replace with your Bigtable instance ID
instance_id = os.getenv("BIGTABLE_INSTANCE")

# Replace with your table name
table_name = "sensor_data"

# The number of rows to generate
num_rows = 1000

# The number of columns to generate
num_columns = 10

# The number of different machines
num_machines = 100

# The number of different sensors
num_sensors = 10

# The number of different sensor types
num_sensor_types = 5

# The number of different measurements
num_measurements = 20

# The time interval between measurements (in seconds)
time_interval = 60

# Create a Bigtable client
client = bigtable.Client(project=project_id, admin=True)
instance = client.instance(instance_id)

# Create a new table in the Bigtable instance
table = instance.table(table_name)

max_age = datetime.timedelta(days=7)

if not table.exists():
    table.create(column_families={
        "machine_info": column_family.MaxAgeGCRule(max_age),
        "sensor_info": column_family.MaxAgeGCRule(max_age),
        "measurement_data": column_family.MaxAgeGCRule(max_age)
    })

# Generate machine information
machine_ids = [f"machine_{i}" for i in range(num_machines)]
machine_types = [f"type_{i}" for i in range(num_machines // 10)]
machine_locations = [f"location_{i}" for i in range(num_machines // 10)]

# Generate sensor information
sensor_ids = [f"sensor_{i}" for i in range(num_sensors)]
sensor_types = [f"type_{i}" for i in range(num_sensor_types)]

# Generate measurement data
measurement_names = [f"measurement_{i}" for i in range(num_measurements)]

# Populate the table with simulated data
for i in range(num_rows):
    row_key = f"{i}"
    machine_id = random.choice(machine_ids)
    machine_type = random.choice(machine_types)
    machine_location = random.choice(machine_locations)
    machine_info = {
        "machine_id": machine_id.encode(),
        "machine_type": machine_type.encode(),
        "machine_location": machine_location.encode()
    }

    sensor_id = random.choice(sensor_ids)
    sensor_type = random.choice(sensor_types)
    sensor_info = {
        "sensor_id": sensor_id.encode(),
        "sensor_type": sensor_type.encode()
    }

    measurement_name = random.choice(measurement_names)
    measurement_value = random.random()
    measurement_timestamp = int(time.time() - (num_rows - i) * time_interval)
    measurement_data = {
        measurement_name: str(measurement_value).encode(),
        "timestamp": str(measurement_timestamp).encode()
    }

    row = table.row(row_key.encode())
    row.set_cell("machine_info", "machine_id", machine_info["machine_id"])
    row.set_cell("machine_info", "machine_type", machine_info["machine_type"])
    row.set_cell("machine_info", "machine_location", machine_info["machine_location"])
    row.set_cell("sensor_info", "sensor_id", sensor_info["sensor_id"])
    row.set_cell("sensor_info", "sensor_type", sensor_info["sensor_type"])
    row.set_cell("measurement_data", measurement_name, measurement_data[measurement_name])
    row.set_cell("measurement_data", "timestamp", measurement_data["timestamp"])

# Print a message to confirm the operation
print(f"The table {table_name} in the Bigtable instance {instance_id} has been populated with {num_rows} rows of simulated manufacturing IoT data.")

