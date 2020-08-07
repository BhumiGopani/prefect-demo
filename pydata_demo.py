import requests
import json
from collections import namedtuple
from contextlib import closing
import sqlite3
from prefect.tasks.database.sqlite import SQLiteScript
from prefect import task, Flow
from prefect.schedules import IntervalSchedule
import datetime
from prefect.engine import signals
from prefect.engine.result_handlers import local_result_handler, LocalResultHandler
import prefect

def alert_failed(obj, old_state, new_sate):
    if new_sate.is_failed():
        print("New State or Flow is Failed!!")

##setup
create_table = SQLiteScript(
    db='cfpbcomplaints.db',
    script='CREATE TABLE IF NOT EXISTS complaint (timestamp TEXT, state TEXT, product TEXT, company TEXT, complaint_what_happened TEXT)'
)

## extract
@task(cache_for=datetime.timedelta(days=1), state_handlers=[alert_failed], result_handler=LocalResultHandler())
def get_complaint_data():
    r = requests.get("https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/", params={'size':10})
    response_json = json.loads(r.text)
    logger = prefect.context.get('logger')
    logger.info("Actually I requested this time")
    return response_json['hits']['hits']

## transform
@task(state_handlers=[alert_failed])
def parse_complaint_data(raw):
    # uncomment below line to see functionality of state handler
    # raise Exception
    # uncomment below line to see functionality of signals
    # raise signals.SUCCESS
    complaints = []
    Complaint = namedtuple('Complaint', ['data_received', 'state', 'product', 'company', 'complaint_what_happened'])
    for row in raw:
        source = row.get('_source')
        this_complaint = Complaint(
            data_received=source.get('date_recieved'),
            state=source.get('state'),
            product=source.get('product'),
            company=source.get('company'),
            complaint_what_happened=source.get('complaint_what_happened')
        )
        complaints.append(this_complaint)
    return complaints

## load
@task(state_handlers=[alert_failed])
def store_complaints(parsed):
    insert_cmd = "INSERT INTO complaint VALUES (?, ?, ?, ?, ?)"
    with closing(sqlite3.connect("cfpbcomplaints.db")) as conn:
        with closing(conn.cursor()) as cursor:

            cursor.executemany(insert_cmd, parsed)
            conn.commit()

schedule = IntervalSchedule(interval=datetime.timedelta(minutes=1))

with Flow("my etl flow", state_handlers=[alert_failed]) as f:
    db_table = create_table()
    raw = get_complaint_data()
    parsed = parse_complaint_data(raw)
    populated_table = store_complaints(parsed)
    populated_table.set_upstream(db_table)

# f.run()
# f.visualize()
f.register()

# For scheduling the flow use: Flow("my etl flow", schedule)