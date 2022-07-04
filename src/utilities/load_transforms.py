from datetime import datetime as dt, timedelta


def time_transform(time_string):
    if time_string == '':
        return None
    else:
        format_string = '%M:%S.%f' if time_string.count(":") == 1 else '%H:%M:%S.%f'
        return None if time_string.find("\\N") > -1 else dt.strptime(time_string, format_string)

def null_transform(data_string):
    return None if data_string.find("\\N") > -1 else data_string

def delta_to_time(time, delta_string):
    if delta_string == '' or delta_string.find("\\N") > -1:
        return None
    else:
        tc = delta_string.replace("+", "").replace(".", ":").split(":")[::-1]
        delta_dict = {
            'hours': 0,
            'minutes': 0,
            'seconds': 0,
            'milliseconds': 0
        }

        for idx, t in enumerate(tc):
            match idx:
                case 0:
                    delta_dict['milliseconds'] = int(t)
                case 1:
                    delta_dict['seconds'] = int(t)
                case 2:
                    delta_dict['minutes'] = int(t)
                case 3:
                    delta_dict['hours'] = int(t)
                case default:
                    break

        # print(delta_dict)
        delta_obj = timedelta(hours=delta_dict['hours'],
                              minutes=delta_dict['minutes'],
                              seconds=delta_dict['seconds'],
                              milliseconds=delta_dict['milliseconds']
                              )

        delta_time = time + delta_obj
        return delta_time
