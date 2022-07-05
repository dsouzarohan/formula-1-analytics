from os.path import join
from datetime import datetime, timedelta

path = "D:\\Documents\\Python Projects\\formula-1-analytics\\logs"


def create_log():
    log = open(join(path, "data_load.log"), "w")
    now = datetime.now()
    print()
    log.write("\\****************************************************\\\n")
    log.write("Beginning full data load at " + now.strftime("%Y-%m-%d %H:%M:%S") + "\n")
    log.write("\\****************************************************\\\n\n\n")
    log.close()
    return now


def log_data_load(table, step, start, count):
    log = open(join(path, "data_load.log"), "a")
    now = datetime.now()
    if step == 'START':
        log.write("Beginning " + table + " data load at " + now.strftime("%Y-%m-%d %H:%M:%S") + "\n")
    elif step == 'END':
        print(start)
        print(now)
        log.write(table + " data load completed at " + now.strftime("%Y-%m-%d %H:%M:%S") + "\n")
        td = timedelta_to_hhmissml(start, now)
        log_message = "Time taken: {:d} hour(s), {:02d} minute(s), {:02d} second(s), {:02d} millisecond(s)\n"
        log.write(log_message.format(td['hours'], td['minutes'], td['seconds'], td['milliseconds']))
        log_message = "Total records inserted: {:d}\n\n\n"
        log.write(log_message.format(count))
    else:
        log.write("\\************************************************************************\\\n")
        log.write("Full data load completed at " + now.strftime("%Y-%m-%d %H:%M:%S") + "\n")
        td = timedelta_to_hhmissml(start, now)
        log_message = "Time taken: {:d} hour(s), {:02d} minute(s), {:02d} second(s), {:02d} millisecond(s)\n"
        log.write(log_message.format(td['hours'], td['minutes'], td['seconds'], td['milliseconds']))
        log.write("\\************************************************************************\\\n")

    log.close()


def timedelta_to_hhmissml(start, end):
    time_taken = (end - start) / timedelta(microseconds=1)
    hours, remainder = divmod(time_taken, 3600000000)
    minutes, remainder = divmod(remainder, 60000000)
    seconds, microseconds = divmod(remainder, 1000000)

    return {
        'hours': int(hours),
        'minutes': int(minutes),
        'seconds': int(seconds),
        'milliseconds': int(microseconds / 1000)
    }
