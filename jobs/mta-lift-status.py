import pandas as pd

from common import RUN_LABEL, get_json, save_csv

# Elevator and escalator status for every Metro-North station, from the API
# behind new.mta.info/elevator-escalator-status. The x-api-key is the public
# key that page sends from every visitor's browser, not a personal secret.
URL = 'https://mnrtraintimeapi.mta.info/wsnk/ttapi/api/stations/lift-equipments'
HEADERS = {
    'accept': 'application/json, text/plain, */*',
    'origin': 'https://new.mta.info',
    'referer': 'https://new.mta.info/elevator-escalator-status',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'x-api-key': '9ea83a8a361efacd098b6c7f6a6e49c1',
}

# API field -> column name, in file order
LIFT_FIELDS = {
    'StationId': 'station_id',
    'StationName': 'station_name',
    'StationNumber': 'station_number',
    'LiftId': 'lift_id',
    'Description': 'description',
    'LiftType': 'lift_type',
    'StatusId': 'status_id',
    'Status': 'status',
    'ReportedDate': 'reported_date',
}

stations = get_json(URL, headers=HEADERS, timeout=60, name='mnr lift equipment')
if not isinstance(stations, list) or not stations:
    raise RuntimeError(f'Unexpected lift equipment response: {str(stations)[:200]}')

# one row per elevator or escalator, with the station's branch attached
rows = []
for kind, field in [('elevator', 'Elevators'), ('escalator', 'Escalators')]:
    for station in stations:
        for lift in station.get(field) or []:
            row = {column: lift.get(name) for name, column in LIFT_FIELDS.items()}
            row['type'] = kind
            row['branch_id'] = station.get('BranchId')
            row['branch_name'] = station.get('BranchName')
            rows.append(row)
if not rows:
    raise RuntimeError('Lift equipment response listed no elevators or escalators')
df = pd.DataFrame(rows)

save_csv(df, 'mta/metro-north-lifts/mta-mnr-lifts-' + RUN_LABEL + '.csv')
