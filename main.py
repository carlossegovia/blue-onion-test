import pandas as pd
from haversine import haversine
from sqlalchemy import create_engine
from pick import pick

table_name = 'starlink'
PG_USER = 'postgres'
PG_PASS = 'postgres'
engine = create_engine(f'postgresql+psycopg2://{PG_USER}:{PG_PASS}@localhost:5433/postgres')


def data_to_postgres(data, table):
    """
    Write DataFrame data to a PostgreSql table
    :param data: Pandas DataFrame
    :param table: Table name
    :return:
    """
    data.to_sql(table, engine, if_exists='replace')


def get_last_position(table, id, time):
    """
    Get the last know satellite position given a Timestamp
    :param table: Table name
    :param id: Satellite id
    :param time: Timestamp
    :return: List with last know position
    """
    with engine.connect() as con:
        rs = con.execute(f"SELECT latitude, longitude FROM {table} WHERE id='{id}' and creation_date='{time}'")
        result = []
        for row in rs:
            result.append({'latitude': row[0], 'longitude': row[1]})
    return result


def get_nearest_satellite(table, time, lat, long):
    """
    Get the nearest satellite to a point (lat, long) given a Timestamp
    :param table: Table name
    :param time: Timestamp
    :param lat: Latitude
    :param long: Longitude
    :return: Nearest satellite data in a dict
    """
    with engine.connect() as con:
        rs = con.execute(f"SELECT id, latitude, longitude FROM {table} WHERE creation_date='{time}'")
        result = []
        for row in rs:
            try:
                result.append(
                    {'id': row[0], 'latitude': row[1], 'longitude': row[2],
                     'distance': haversine((lat, long), (row[1], row[2]))})
            except Exception:
                print(f"Can't calculate distance between point ({row[1]}, {row[2]}) and ({lat}, {long})")
    df = pd.DataFrame(result)
    nearest = None if df.empty else df[df['distance'] == df['distance'].min()].to_dict('records')[0]
    return nearest


def show_menu():
    """
    Show menu and options
    """
    title = 'Please choose an Option:'
    options = ['Load data to PostgreSql', 'Get satellite last know position', 'Get nearest satellite by time', 'Exit']
    return pick(options, title, indicator='=>', default_index=0)


if __name__ == '__main__':
    option, index = show_menu()
    while index != 3:
        if index == 0:
            # read historical data
            df = pd.read_json('./starlink_historical_data.json')
            df['creation_date'] = df['spaceTrack'].apply(lambda d: d['CREATION_DATE'])
            df = df[['id', 'creation_date', 'latitude', 'longitude']]
            # insert into Postgres
            data_to_postgres(df, table_name)
            print("Data was successfully loaded\n\n")
            input("Press to continue")
        elif index == 1:
            # get last position
            # get_last_position(table_name, '5eed7714096e59000698563d', '2020-09-05T23:56:09')
            # get_last_position(table_name, '5eefa85e6527ee0006dcee24', '2020-02-20T21:36:25')
            sat_arg = input('Satellite id: ')
            time_arg = input("Creation date: ")
            positions = get_last_position(table_name, sat_arg, time_arg)
            if len(positions) > 0:
                print(f'Satellite {sat_arg} last know position(s) given time: {time_arg}')
                for p in positions:
                    print(f'({p["latitude"]}, {p["longitude"]})')
            else:
                print('No results for the given arguments')
            input("\nPress to continue")
        else:
            # print(get_nearest_satellite(table_name, '2021-01-26T06:36:09', 52.78, 95))
            time_arg = input("Creation date: ")
            lat_arg = float(input("Latitude: "))
            long_arg = float(input("Longitude: "))
            result = get_nearest_satellite(table_name, time_arg, lat_arg, long_arg)
            if result:
                print(f'{result["id"]} is the nearest satellite with a distance of {result["distance"]} Km')
            else:
                print('No results for the given arguments')
            input("\nPress to continue")
        option, index = show_menu()
