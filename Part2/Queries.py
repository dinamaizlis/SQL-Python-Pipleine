from datetime import datetime
def Queries(cursor):

    # Query 1:
    cursor.execute("SELECT stn_name FROM 10MinutesObservation ORDER BY TD DESC LIMIT 1")
    result = cursor.fetchone()
    print(f"Station with the highest temperature: {result[0]}")


    # Query 2:
    cursor.execute(f'''
    SELECT stn_name, AVG(TD) as avg_td
    FROM (
        SELECT stn_name, TD, ROW_NUMBER() OVER (PARTITION BY stn_name ORDER BY time_obs DESC) as row_num
        FROM 10MinutesObservation
    ) ranked
    WHERE row_num <= 3
    GROUP BY stn_name
    ORDER BY AVG(TD) DESC
    LIMIT 1
''')
    result = cursor.fetchone()
    print(f"Station with the highest average level: {result[0]} Average level: {result[1]}")


    # Query 3:
    today_date = datetime.now().date()
    cursor.execute(f'''
        SELECT AVG(TD) as avg_td
        FROM 10MinutesObservation
        WHERE DATE(time_obs) = '{today_date}'
    ''')
    result = cursor.fetchone()
    print(f"Average TD for today: {result[0]}")









