
from pyspark import SparkContext


def filterBike(pId, lines):
    import csv
    for row in csv.reader(lines):
        if (row[6] == 'Greenwich Ave & 8 Ave' and row[3].startswith('2015-02-01')):
            yield (row[3][:19])


def filterTaxi(pId, lines):
    if pId == 0:
        next(lines)

    import csv
    import pyproj
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    gLoc = proj(-74.00263761, 40.73901691)
    sqm = 1320 ** 2
    for row in csv.reader(lines):
        try:
            dropoff = proj(float(row[5]), float(row[4]))
        except:
            continue
        distance = (dropoff[0] - gLoc[0]) ** 2 + (dropoff[1] - gLoc[1]) ** 2
        if distance < sqm:
            yield row[1][:19]


def findTrips(_, records):
    import datetime
    lastTaxiTime = None
    for dt, event in records:
        t = datetime.datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
        if event == 1:
            if lastTaxiTime != None:
                if (t - lastTaxiTime).total_seconds() < 600:
                    yield(dt, event)
        else:
            lastTaxiTime = t


if __name__ == '__main__':
    sc = SparkContext()
    taxi = sc.textFile('/data/share/bdm/yellow.csv.gz')
    bike = sc.textFile('/data/share/bdm/citibike.csv')

    gBike = bike.mapPartitionsWithIndex(filterBike).cache()
    gTaxi = taxi.mapPartitionsWithIndex(filterTaxi).cache()

    gAll = (gTaxi.map(lambda x: (x, 0)) + gBike.map(lambda x: (x, 1)))

    print(gAll.sortByKey().mapPartitionsWithIndex(findTrips).count())
