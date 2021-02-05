### Aquatic_Sensing via MQTT ###
#### create_table.py ####
>1. Create Tables by using this Script.
>2. Create an Empty Database Schema, Database support MySQL or MariaDB.
>3. This Script use ORM via Python SQLAlchemy Package.

#### Getting_Data.py ####
>1. Main file for querying and storing data .
>2. Use MQTT to communicate with sensor.
>3. Data store at local and remote database, also store as file at local folder.

#### Query_Data.py ####
>1. Schedule Query Data .
>2. Daily report.
>3. Query data from database view, also update to google drive folder.