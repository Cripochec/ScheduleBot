import DB
import time


start = time.time()

con = DB.connect()
schedule = DB.json_schedule()
DB.clear_tables_schedule(con)
DB.parsing_schedule(con, schedule)
DB.disconnect(con)

print(f'------{time.time() - start} second.------')