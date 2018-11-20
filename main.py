# Group members: Yue Sheng, Yazhuo Zhang
# Yazhuo Zhang is in charge of reading data from file incrementally, and store each storm data at a time.
# Yue Sheng is in charge of processing the storm data to get needed information.

from collections import Counter
from pygeodesy import ellipsoidalVincenty as ev


def parse_records(headline, records, range2true):

    total_distance = 0
    propagation_sum = 0
    max_propagation = 0
    sub_count = 0
    total_hour_difference = 0

    if len(records) > 0:
        for i in range(len(records)):
            # last record, skip
            if i == len(records) - 1:
                break

            # get latitude and longitude
            cur_latitude = records[i].split(',')[4].strip()
            cur_longitude = records[i].split(',')[5].strip()
            print("cur_latitude")
            print(cur_latitude)
            print(cur_longitude)
            cur_position = ev.LatLon(cur_latitude, cur_longitude)
            next_latitude = records[i+1].split(',')[4].strip()
            next_longitude = records[i+1].split(',')[5].strip()
            next_position = ev.LatLon(next_latitude, next_longitude)

            # calculate distance
            distance = cur_position.distanceTo(next_position) / 1852.0
            total_distance += distance

            # calculate speed
            day_difference = int(records[i+1].split(',')[0].strip()) - int(records[i].split(',')[0].strip())
            minute_difference = int(records[i+1].split(',')[1].strip()) - int(records[i].split(',')[1].strip())
            hour_difference = (day_difference * 2400 + minute_difference) / 100
            speed = distance / hour_difference

            total_hour_difference += hour_difference

            propagation_sum += speed
            if speed > max_propagation:
                max_propagation = speed
            print(speed)

            # investigate scientific hypothesis
            bearing = cur_position.bearingTo(next_position)
            max_quadrant = []
            flag = True  # whether segment is eligible
            for j in range(2, -1, -1):  # from 64, 50 to 34-kt
                start = 8 + j * 4
                northeastern_extent = int(records[i].split(',')[start].strip())
                southeastern_extent = int(records[i].split(',')[start+1].strip())
                southwestern_extent = int(records[i].split(',')[start+2].strip())
                northwestern_extent = int(records[i].split(',')[start+3].strip())

                if northeastern_extent == -999:  # invalid
                    flag = False
                    break

                if northeastern_extent + southeastern_extent + southwestern_extent + northwestern_extent == 0:
                    if j == 0:  # no wind, not eligible segment
                        flag = False
                    continue
                else:  # find expected quadrants
                    max_extent = max(northeastern_extent, southeastern_extent, southwestern_extent, northwestern_extent)
                    if northeastern_extent == max_extent:
                        max_quadrant.append(1)
                    if southeastern_extent == max_extent:
                        max_quadrant.append(2)
                    if southwestern_extent == max_extent:
                        max_quadrant.append(3)
                    if northwestern_extent == max_extent:
                        max_quadrant.append(4)
                    break

            if not flag:
                continue

            # eligible segment
            sub_count += 1
            for k in range(70, 111):
                degree = (bearing + k) % 360
                if 0 <= degree <= 90 and 1 in max_quadrant or 90 <= degree <= 180 and 2 in max_quadrant \
                        or 180 <= degree <= 270 and 3 in max_quadrant or (270 <= degree <= 359 or degree == 0) and 4 in max_quadrant:
                    range2true[k] += 1
                    continue

        if total_hour_difference != 0:
            mean_speed = total_distance/total_hour_difference
            print(mean_speed)

    return sub_count


def analyze(src):
    f = open(src)
    line = f.readline()
    records = []
    headline = ""

    range2true = Counter()
    eligible_count = 0

    # read data line by line
    while line:
        if line[0].isalpha():
            if records:
                eligible_count += parse_records(headline, records, range2true)
                records = []

            headline = line
        else:
            records.append(line)

        line = f.readline()

    # process the final storm data
    if records:
        eligible_count += parse_records(headline, records, range2true)

    print(eligible_count)
    for k in range(70, 111):
        print(k, range2true[k])

    print()
    f.close()


def main():
    print("Start to analyze Atlantic storm data:\n")
    analyze("Atlantic.txt")
    print("Start to analyze Pacific storm data:\n")
    analyze("Pacific.txt")
main()
