import DB
# import time
import logging


logging.basicConfig(level=logging.INFO, filename="py_log.log", filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")


def pars_data():
    con = DB.connect()
    schedule = DB.json_schedule()
    DB.clear_tables(con)
    DB.parsing_groups(con)
    DB.parsing_audience(con)
    DB.parsing_lecturers(con, schedule)
    DB.parsing_type_disciplines(con, schedule)
    DB.parsing_disciplines(con, schedule)
    DB.parsing_calls(con, schedule)
    DB.disconnect(con)


def pars_data_schedule():
    con = DB.connect()
    schedule = DB.json_schedule()
    DB.clear_tables_schedule(con)
    DB.parsing_schedule(con, schedule)
    DB.disconnect(con)


def data_processing(line):
    schedule = []
    for row in line:
        arr = {'week': row[0], 'date': row[1].strftime("%d.%m.%Y"), 'lesson': row[2], 'time': row[3],
               'discipline': row[4], 'type': row[5], 'aud': row[6], 'lec': row[7]}
        schedule.append(arr)
    sort = sorted(schedule, key=lambda x: (x['date'], x['lesson']))
    rasp = ''
    date = ''
    for row in sort:
        if date != row['date']:
            date = row['date']
            rasp += f"_________________________________________________________________\n{row['week']}  {row['date']}\n"
        rasp += f"{row['lesson']}. {row['time']} - {row['discipline']} ({row['type']}) " \
                f"{row['aud']} {row['lec']}\n"
    return rasp


def get_schedule_today():
    con = DB.connect()
    schedule = data_processing(DB.today(con))
    DB.disconnect(con)
    if schedule == '':
        schedule = 'Сергей, сегодня выходной'
    return schedule


def get_schedule_tomorrow():
    con = DB.connect()
    schedule = data_processing(DB.tomorrow(con))
    DB.disconnect(con)
    if schedule == '':
        schedule = 'Сергей, завтра выходной'
    return schedule


def get_schedule_week():
    con = DB.connect()
    schedule = data_processing(DB.week(con))
    DB.disconnect(con)
    return schedule


# pars_data()
# pars_data_schedule()

# КОД	КАТЕГОРИЯ РЕЗУЛЬТАТА
# 1хх	Информационный ответ
# 2хх	Успешная операция
# 3хх	Перенаправление
# 4хх	Ошибка на стороне клиента
# 5хх	Ошибка на стороне сервера

# table - groups
# form: 5-зфо, 4-офо, 6-озфо
# level (уровень образования): 5-аспирантура, 0-бакалавриат, 1-магистратура


# Исключение проверки при новом дне
# Исключение при первом запуске
# Какой сегодня праздник

