import sys
import re
from collections import Counter
import operator
import datetime
import time


logFile = sys.argv[1]
hostFile = sys.argv[2]
hoursFile = sys.argv[3]
resourceFile = sys.argv[4]
blockedFile = sys.argv[5]

ip_list = []
resource_dict = {}
hours_list = []
hours_period = 0
ip_dict = {}
blocked_ip_dict = {}
blocked_requests = []

# Convert UTC to POSIX
def posix(utc_datetime):
    return datetime.datetime.strptime(utc_datetime, "%d/%b/%Y:%H:%M:%S %z").timestamp()

start_time = time.time()

# Open log file and process data
with open(logFile, 'r+', encoding="latin-1") as file:

    line_number = 0;

    for line in file:

        line_number += 1

        # Extracting Feature 1
        ip = re.search("^(.*?)(?=\s)",line).group()
        ip_list.append(ip)

        # Extracting  Feature 2
        resource = re.search("(?=\").+$",line).group()
        resource_path = re.search("\/.*(?=\")", resource)

        if resource_path:
            if "HTTP" in resource_path.group():
                resource_path = re.search("\/.*(?=HTTP)", resource_path.group()).group()
            else:
                resource_path = resource_path.group()
        else:
            resource_plath = " "

        resource_bytes = re.search("(\d+|-)$", resource).group()

        if resource_bytes == "-":
            resource_bytes = 0

        if resource_path not in resource_dict:
            resource_dict[resource_path] = int(resource_bytes)
        else:
            resource_dict[resource_path] += int(resource_bytes)

        # Extracing Feature 3
        datetime_regex = "(?<=\[)\d{2}\/\w{3}\/\d{4}:\d{2}:\d{2}:\d{2}\s-\d{4}"
        server_datetime = re.search(datetime_regex, line).group()
        posix_datetime = posix(server_datetime)

        if len(hours_list) == 0:
            hours_list.append([server_datetime, 0])

        start_hours = posix(hours_list[hours_period][0])
        end_hours = start_hours + 3600

        if posix_datetime <= end_hours:
            hours_list[hours_period][1] += 1
        else:
            hours_period += 1
            hours_list.append([server_datetime, 1])

        # Extracting Feature 4
        fail_login = re.search("POST\s\/login\sHTTP/1.0\"\s401", line)
        success_login = re.search("POST\s\/login\sHTTP/1.0\"\s200", line)

        if ip in blocked_ip_dict:
            if posix_datetime <= blocked_ip_dict[ip][1]:
                blocked_requests.append(line)
            elif posix_datetime > blocked_ip_dict[ip][1]:
                blocked_ip_dict.pop(ip)
                del ip_dict[ip]

        if success_login:
            if (ip in ip_dict) and (ip not in blocked_ip_dict):
                    del ip_dict[ip]

        if fail_login:
            if (ip in ip_dict) and (posix_datetime <= ip_dict[ip][2]):
                if ip_dict[ip][3] == 2:
                    blocked_ip_dict[ip] = [ip, posix_datetime + 300]
                else:
                    ip_dict[ip][3] += 1
            else:
                ip_dict[ip] = [line, posix_datetime, posix_datetime + 20, 1]




# Prepare data to be writen on output file
ip_dict = Counter(ip_list)
ip_top = ip_dict.most_common(10)

resource_dict_sorted = sorted(resource_dict.items(), key=operator.itemgetter(1), reverse=True)

hours_list_sorted = sorted(hours_list, key=operator.itemgetter(1), reverse=True)

# Write Feature 1 results to file
with open(hostFile,"w") as file:
    for index in range(len(ip_top)):
        file.writelines(str(ip_top[index][0]) + "," + str(ip_top[index][1]) + "\n")


# Write Feature 2 results to file
with open(resourceFile, "w") as file:
    for index in range(len(resource_dict_sorted)):
        if index == 10:
            break
        else:
            file.writelines(str(resource_dict_sorted[index][0]) + "\n")

# Write Feature 3 results to file
with open(hoursFile, "w") as file:
    for index in range(len(hours_list_sorted)):
        if index == 10:
            break
        else:
            file.writelines(str(hours_list_sorted[index][0]) + "," + str(hours_list_sorted[index][1])+"\n")

# Write Feature 4 results to file
with open(blockedFile, "w") as file:
    for index in range(len(blocked_requests)):
        file.writelines(str(blocked_requests[index]))

elapsed_time = time.time() - start_time
print(elapsed_time)
