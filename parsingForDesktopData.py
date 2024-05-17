import DB
import time


start = time.time()

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

print(f'------{time.time() - start} second.------')

