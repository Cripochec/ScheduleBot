import psycopg2
import requests
from datetime import datetime, timedelta
import logging
from config import HOST, USER, PASSWORD, DB_NAME, SCHEDULE, GROUPS, AUDIENCES


# Establishes a connection to the PostgreSQL database
def connect():
    try:
        connection = psycopg2.connect(
            user=USER,
            password=PASSWORD,
            host=HOST,
            database=DB_NAME
        )
        connection.autocommit = True
        return connection
    except (Exception, psycopg2.Error) as error:
        logging.error('[ERROR] Could not connect to database:', exc_info=error)
        return None


# Closes the connection to the PostgreSQL database
def disconnect(connection):
    if connection:
        connection.close()


# Clears the audiences, teachers, groups, disciplines tables
def clear_tables(connection):
    try:
        with connection.cursor() as cursor:
            cursor.execute("TRUNCATE groups CASCADE")
            cursor.execute("TRUNCATE audience CASCADE")
            cursor.execute("TRUNCATE lecturers CASCADE")
            cursor.execute("TRUNCATE type_disciplines CASCADE")
            cursor.execute("TRUNCATE disciplines CASCADE")
            cursor.execute("TRUNCATE calls CASCADE")
            logging.info('tables groups, audience, lecturers, typeDisciplines, disciplines, calls  are cleared')
    except (Exception, psycopg2.Error) as error:
        logging.error('Could not clear tables:', exc_info=error)


# Clears schedule tables
def clear_tables_schedule(connection):
    try:
        with connection.cursor() as cursor:
            cursor.execute("TRUNCATE schedule;")
            logging.info('tables schedule  are cleared')
    except (Exception, psycopg2.Error) as error:
        logging.error('Could not clear table "schedule":', exc_info=error)


# Json schedule request
def json_schedule():
    try:
        response = requests.get(f"{GROUPS}")
        group = response.json()
        schedule = []
        for row in group:
            response = requests.get(f"{SCHEDULE}?&v_gru={row['oid']}")
            if not response.json():
                continue
            for oid in response.json():
                arr = {'date': oid['date'], 'time': oid['time'], 'lesson': oid['lesson'], 'group': row['oid'],
                       'content': {'disciplina': oid['content']['disciplina'],
                                   'type_disciplina': oid['content']['type_disciplina'],
                                   'aud': oid['content']['aud'],
                                   'lecturer': oid['content']['lecturer'],
                                   'subgroupname': oid['content']['subgroupname']}}
                schedule.append(arr)
        logging.info(f'Json query results "schedule" for all groups": {str(response)}')
        return schedule

    except (Exception, psycopg2.Error) as error:
        logging.error('Json request "schedule" for all groups failed: ', exc_info=error)


# Filling in the "groups" table
def parsing_groups(connection):
    try:
        response = requests.get(f"{GROUPS}")
        logging.info(f'Json query results "GROUPS": {str(response)}')
        group = response.json()
        with connection.cursor() as cursor:
            for row in group:
                cursor.execute(f"""
                    INSERT INTO groups(id_group, name_group)
                    VALUES('{row['oid']}', '{row['name']}');""")
    except (Exception, psycopg2.Error) as error:
        logging.error('Unable to populate "groups" table: ', exc_info=error)


# Filling in the "audience" table
def parsing_audience(connection):
    try:
        response = requests.get(f"{AUDIENCES}")
        logging.info(f'Json query results "AUDIENCES": {str(response)}')
        audience = response.json()
        with connection.cursor() as cursor:
            for row in audience:
                cursor.execute(f"""
                    INSERT INTO audience(id_audience, name_audience)
                    VALUES('{row['oid']}', '{row['name']}');""")
    except (Exception, psycopg2.Error) as error:
        logging.error('Unable to populate "audience" table: ', exc_info=error)


# Filling in the "lecturers" table
def parsing_lecturers(connection, schedule):
    try:
        seen_ids = []
        for row in schedule:
            if row['content']['lecturer'] not in seen_ids:
                seen_ids.append(row['content']['lecturer'])
        with connection.cursor() as cursor:
            for row in seen_ids:
                cursor.execute(f"""
                            INSERT INTO lecturers(fio)
                            VALUES('{row}');""")
    except (Exception, psycopg2.Error) as error:
        logging.error('Unable to populate "lecturers" table: ', exc_info=error)


# Filling in the "type_disciplines" table
def parsing_type_disciplines(connection, schedule):
    try:
        seen_ids = []
        for row in schedule:
            if row['content']['type_disciplina'] not in seen_ids:
                seen_ids.append(row['content']['type_disciplina'])
        with connection.cursor() as cursor:
            for row in seen_ids:
                cursor.execute(f"""
                    INSERT INTO type_disciplines(name_type)
                    VALUES('{row}');""")

    except (Exception, psycopg2.Error) as error:
        logging.error('Unable to populate "type_disciplines" table: ', exc_info=error)


# Filling in the "disciplines" table
def parsing_disciplines(connection, schedule):
    try:
        seen_ids = []
        for row in schedule:
            if row['content']['disciplina'] not in seen_ids:
                seen_ids.append(row['content']['disciplina'])
        with connection.cursor() as cursor:
            for row in seen_ids:
                cursor.execute(f"""
                    INSERT INTO disciplines(name_discipline)
                    VALUES('{row}');""")

    except (Exception, psycopg2.Error) as error:
        logging.error('Unable to populate "disciplines" table: ', exc_info=error)


# Filling in the "calls" table
def parsing_calls(connection, schedule):
    try:
        seen_ids = []
        for row in schedule:
            arr = {'lesson': row['lesson'], 'lesson_time': row['time']}
            if arr not in seen_ids:
                seen_ids.append(arr)
        with connection.cursor() as cursor:
            for row in seen_ids:
                cursor.execute(f"""
                    INSERT INTO calls(lesson, lesson_time)
                    VALUES('{row['lesson']}', '{row['lesson_time']}');""")

    except (Exception, psycopg2.Error) as error:
        logging.error('Unable to populate "calls" table: ', exc_info=error)


# Filling in the "schedule" table
def parsing_schedule(connection, schedule):
    try:
        with connection.cursor() as cursor:
            for gru in schedule:
                record = []

                # Date
                record.append(str(gru['date']))

                # Groups
                record.append(gru['group'])

                # Subgroup
                if gru['content']['subgroupname'] is not None:
                    record.append(gru['content']['subgroupname'][-2])
                else:
                    record.append(0)

                # Lesson number
                cursor.execute(f"""SELECT id_call FROM calls 
                    WHERE lesson = {gru['lesson']} and lesson_time = '{gru['time']}';""")
                less = cursor.fetchall()
                record.append(less[0][0])

                # Discipline
                cursor.execute(f"""SELECT id_discipline FROM disciplines 
                WHERE name_discipline = '{gru['content']['disciplina']}'""")
                dis = cursor.fetchall()
                record.append(dis[0][0])

                # Discipline type
                cursor.execute(f"""SELECT id_type_discipline FROM type_disciplines 
                WHERE name_type = '{gru['content']['type_disciplina']}'""")
                typ = cursor.fetchall()
                record.append(typ[0][0])

                # Audience
                cursor.execute(f"""SELECT id_audience FROM audience 
                WHERE name_audience = '{gru['content']['aud']}'""")
                aud = cursor.fetchall()
                record.append(aud[0][0])

                # Lecturer
                cursor.execute(f"""SELECT id_lecturer FROM lecturers 
                WHERE fio = '{gru['content']['lecturer']}'""")
                lec = cursor.fetchall()
                record.append(lec[0][0])

                # Schedule
                cursor.execute(f"""INSERT INTO schedule (dates, id_group, subgroup, id_call,
                id_discipline, id_type_discipline, id_audience, id_lecturer)
                VALUES('{record[0]}', {record[1]}, {record[2]}, {record[3]}, {record[4]},
                {record[5]}, {record[6]}, {record[7]});""")

    except (Exception, psycopg2.Error) as error:
        logging.error('Unable to populate "schedule" table: ', exc_info=error)


# Get schedules for today
def today(connection):
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"""
            SELECT name_day_week, dates, lesson, lesson_time, name_discipline, name_type, name_audience, fio
            
            FROM schedule, days_week, groups, calls, disciplines, type_disciplines, audience, lecturers
            
            WHERE schedule.id_day_week = days_week.id_day_week and schedule.id_call = calls.id_call and 
            schedule.id_discipline = disciplines.id_discipline and schedule.id_group = groups.id_group and
            schedule.id_type_discipline = type_disciplines.id_type_discipline and 
            schedule.id_audience = audience.id_audience and schedule.id_lecturer = lecturers.id_lecturer and 
            schedule.id_group = 4500 and schedule.dates = '{datetime.now().date()}' 
            and (schedule.subgroup = 2 or schedule.subgroup = 0)""")
            # '{str(datetime.now().date)}'
            line = cursor.fetchall()
            return line

    except (Exception, psycopg2.Error) as error:
        logging.error('today schedule request failed: ', exc_info=error)


# Get schedules for tomorrow
def tomorrow(connection):
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"""
            SELECT name_day_week, dates, lesson, lesson_time, name_discipline, name_type, name_audience, fio

            FROM schedule, days_week, groups, calls, disciplines, type_disciplines, audience, lecturers

            WHERE schedule.id_day_week = days_week.id_day_week and schedule.id_call = calls.id_call and 
            schedule.id_discipline = disciplines.id_discipline and schedule.id_group = groups.id_group and
            schedule.id_type_discipline = type_disciplines.id_type_discipline and 
            schedule.id_audience = audience.id_audience and schedule.id_lecturer = lecturers.id_lecturer and 
            schedule.id_group = 4500 and schedule.dates = '{(datetime.now() + timedelta(days=1)).date()}' 
            and (schedule.subgroup = 2 or schedule.subgroup = 0)""")
            line = cursor.fetchall()
            return line

    except (Exception, psycopg2.Error) as error:
        logging.error('tomorrow schedule request failed: ', exc_info=error)


# Get schedules for week
def week(connection):
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"""
            SELECT name_day_week, dates, lesson, lesson_time, name_discipline, name_type, name_audience, fio

            FROM schedule, days_week, groups, calls, disciplines, type_disciplines, audience, lecturers

            WHERE schedule.id_day_week = days_week.id_day_week and schedule.id_call = calls.id_call and 
            schedule.id_discipline = disciplines.id_discipline and schedule.id_group = groups.id_group and
            schedule.id_type_discipline = type_disciplines.id_type_discipline and 
            schedule.id_audience = audience.id_audience and schedule.id_lecturer = lecturers.id_lecturer and 
            schedule.id_group = 4500 and (schedule.subgroup = 2 or schedule.subgroup = 0)""")
            line = cursor.fetchall()
            return line

    except (Exception, psycopg2.Error) as error:
        logging.error('week schedule request failed: ', exc_info=error)
# Для оптимизации кода можно использовать следующие подходы:
#
# Для множественной вставки данных использовать метод executemany, который позволяет вставлять несколько строк за один раз.
#
# Для выполнения множественных запросов использовать метод execute_batch, который позволяет выполнить несколько запросов за один раз.
#
# Использовать генераторы списков и множеств для ускорения работы со списками и избежания повторения элементов.
#
#
#
# import psycopg2
# import requests
# import datetime
# import logging
# from config import HOST, USER, PASSWORD, DB_NAME, SCHEDULE
#
#
# def fetch_schedules(groups):
#     unique_data = []
#     seen_ids = []
#
#     for group in groups:
#         response = requests.get(f"{SCHEDULE}?&v_gru={group['oid']}")
#         schedule = response.json()
#
#         for i in schedule:
#             content = i['content']
#             if content['lecturer'] not in seen_ids:
#                 unique_data.append(content['lecturer'])
#                 seen_ids.append(content['lecturer'])
#
#             if content['disciplina'] not in seen_ids:
#                 unique_data.append(content['disciplina'])
#                 seen_ids.append(content['disciplina'])
#
#             if content['type_disciplina'] not in seen_ids:
#                 unique_data.append(content['type_disciplina'])
#                 seen_ids.append(content['type_disciplina'])
#
#     return unique_data
#
#
# def pars_new_data(aud, teach, group):
#     try:
#         # Connect to PostgreSQL database
#         with psycopg2.connect(
#             user=USER,
#             password=PASSWORD,
#             host=HOST,
#             database=DB_NAME
#         ) as connection:
#             connection.autocommit = True
#
#             # Clear tables
#             with connection.cursor() as cursor:
#                 cursor.execute("""
#                     TRUNCATE audiences CASCADE;
#                     TRUNCATE teachers CASCADE;
#                     TRUNCATE groups CASCADE;
#                     TRUNCATE disciplines CASCADE;
#                     TRUNCATE type_disciplina CASCADE;
#                 """)
#             logging.info('Tables cleared')
#
#             # Add data for audiences
#             with connection.cursor() as cursor:
#                 for a in aud:
#                     cursor.execute(
#                         "INSERT INTO audiences (id_aud, num_aud) VALUES (%s, %s)",
#                         (a['oid'], a['name'])
#                     )
#                 logging.info('Data added for audiences')
#
#             # Add data for teachers
#             with connection.cursor() as cursor:
#                 unique_data = fetch_schedules(group)
#
#                 for t in unique_data:
#                     cursor.execute(
#                         "INSERT INTO teachers (full_name_teach) VALUES (%s)",
#                         (t,)
#                     )
#                 logging.info('Data added for teachers')
#
#             # Add data for groups
#             with connection.cursor() as cursor:
#                 for g in group:
#                     cursor.execute(
#                         "INSERT INTO groups (id_gr, gr, form, level) VALUES (%s, %s, %s, %s)",
#                         (g['oid'], g['name'], g['form'], g['level'])
#                     )
#                 logging.info('Data added for groups')
#
#             # Add data for disciplines
#             with connection.cursor() as cursor:
#                 unique_data = fetch_schedules(group)
#
#                 for d in unique_data:
#                     cursor.execute(
#                         "INSERT INTO disciplines (dis) VALUES (%s)",
#                         (d,)
#                     )
#                 logging.info('Data added for disciplines')
#
#             # Add data for type_disciplina
#             with connection.cursor() as cursor:
#                 unique_data = fetch_schedules(group)
#
#                 for td in unique_data:
#                     cursor.execute(
#                         "INSERT INTO type_disciplina (type_name) VALUES (%s)",
#                         (td,)
#                     )
#                 logging.info('Data added for type_disciplina')
#
#     except (Exception, psycopg2.Error) as error:
#         logging.error('