"""Read a Plantower PMS7003 serial sensor data.

A simple script to test the Plantower PMS7003 serial particulate sensor.

Outputs the various readings to the console.

Requires PySerial.

Edit the physical port variable to match the port you are using to connect to the sensor.

Updated to work on Python 3 (ord() is redundant)

Run with 'python pms7003.py'

The sensor payload is 32 bytes:

2 fixed start bytes (0x42 and 0x4d)
2 bytes for frame length
6 bytes for standard concentrations in ug/m3 (3 measurements of 2 bytes each)
6 bytes for atmospheric concentrations in ug/m3 (3 measurements of 2 bytes each)
12 bytes for counts per 0.1 litre (6 measurements of 2 bytes each)
1 byte for version
1 byte for error codes
2 bytes for checksum

MIT License

Copyright (c) 2018 Mark Benson

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

"""

import os
import time
import Adafruit_DHT
import logging
import serial
import pymysql


physicalPort = '/dev/ttyS0'

serialPort = serial.Serial(physicalPort)  # open serial port

logging.basicConfig(filename='/home/pi/src/sensing/pms_th_toserver.log', level=logging.INFO)

min_gap=4.9


av_concPM1_0_CF1 = 0
av_concPM2_5_CF1 = 0
av_concPM10_0_CF1 = 0 
# Atmospheric partic
av_concPM1_0_ATM = 0
av_concPM2_5_ATM = 0
av_concPM10_0_ATM = 0
# Raw counts per 0.1
av_rawGt0_3um = 0 
av_rawGt0_5um = 0 
av_rawGt1_0um = 0 
av_rawGt2_5um = 0 
av_rawGt5_0um = 0 
av_rawGt10_0um = 0
#humi/temp
av_humi = 0
av_temp = 0

samples = 0
th_samples = 0
gap=time.time()
temp = 0
humi = 0
# os.system('clear')  # Set to 'cls' on Windows, 'clear' on linux
db = None
cursor = None

def dbConnect():
    global db
    # Open database connection
    db = pymysql.connect("localhost","data_acq","data_acq","env_data" )
    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    # execute SQL query using execute() method.
    cursor.execute("SELECT VERSION()")

    # Fetch a single row using fetchone() method.
    data = cursor.fetchone()
    logging.info("Connected! Database version : %s " % data)



def dbClose():
    global db
    # disconnect from server
    if db!=None:
        db.close()


def displayData():
    # DISPLAY DATA
    #------------------------
    # Clear the screen before displaying the next set of data
    os.system('clear')  # Set to 'cls' on Windows, 'clear' on linux
    print("DateTime: " + now )
    print("AM2301 Sensor Data: ")
    print("----------------------------------------")
    print("Temp = " + str(round(av_temp,1)) + "\u00b0C")
    print("Humi = " + str(round(av_humi,0)) + "%")
    print("\n\n")
    print("PMS7003 Sensor Data [Version " + str(version) + "]")
    print("----------------------------------------")
    print("PM1.0 = " + str(av_concPM1_0_CF1) + " ug/m3")
    print("PM2.5 = " + str(av_concPM2_5_CF1) + " ug/m3")
    print("PM10  = " + str(av_concPM10_0_CF1) + " ug/m3")
    print("PM1 ATM   = " + str(av_concPM1_0_ATM) + " ug/m3")
    print("PM2.5 ATM = " + str(av_concPM2_5_ATM) + " ug/m3")
    print("PM10 ATM  = " + str(av_concPM10_0_ATM) + " ug/m3")
    print("Count: 0.3um = " + str(av_rawGt0_3um) + " per 0.1l")
    print("Count: 0.5um = " + str(av_rawGt0_5um) + " per 0.1l")
    print("Count: 1.0um = " + str(av_rawGt1_0um) + " per 0.1l")
    print("Count: 2.5um = " + str(av_rawGt2_5um) + " per 0.1l")
    print("Count: 5.0um = " + str(av_rawGt5_0um) + " per 0.1l")
    print("Count: 10um  = " + str(av_rawGt10_0um) + " per 0.1l")


def dbPushData():
    global db
    # prepare a cursor object using cursor() method
    if db!=None:
        cursor = db.cursor()

        # Prepare SQL query to INSERT a record into the database.
        sql = """INSERT INTO living VALUES ('{}',{},{},{},{},{},{},{},{},{},{},{},{},{},{})""".format(now, av_concPM1_0_CF1, av_concPM2_5_CF1, av_concPM10_0_CF1, av_concPM1_0_ATM, av_concPM2_5_ATM, av_concPM10_0_ATM, av_rawGt0_3um, av_rawGt0_5um, av_rawGt1_0um, av_rawGt2_5um, av_rawGt5_0um, av_rawGt10_0um, av_temp, av_humi)
        logging.debug(sql)
        try:
            # Execute the SQL command
            cursor.execute(sql)
            # Commit your changes in the database
            db.commit()
            logging.debug("data committed")
        except:
            # Rollback in case there is any error
            db.rollback()
            logging.error("in dbPushData. The database is being rolled back")
            raise




if __name__ == "__main__":
    
    # CONNECT TO DATABASE AND START LOOP
    # ----------------------------------
    logging.info("-----------------------------------------")
    logging.info("pms_th_toserver launched at {}".format(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())))
    dbConnect()
    
    try:
        while True:
            # Check if we have enough data to read a payload
            if serialPort.in_waiting >= 32:
                # Check that we are reading the payload from the correct place (i.e. the start bits)
                if ord(serialPort.read()) == 0x42 and ord(serialPort.read()) == 0x4d:
    
                    # Read the remaining payload data
                    data = serialPort.read(30)
    
                    # Extract the byte data by summing the bit shifted high byte with the low byte
                    # Use ordinals in python to get the byte value rather than the char value
                    frameLength = data[1] + (data[0] << 8)
                    # Standard particulate values in ug/m3
                    concPM1_0_CF1 = data[3] + (data[2] << 8)
                    concPM2_5_CF1 = data[5] + (data[4] << 8)
                    concPM10_0_CF1 = data[7] + (data[6] << 8)
                    # Atmospheric particulate values in ug/m3
                    concPM1_0_ATM = data[9] + (data[8] << 8)
                    concPM2_5_ATM = data[11] + (data[10] << 8)
                    concPM10_0_ATM = data[13] + (data[12] << 8)
                    # Raw counts per 0.1l
                    rawGt0_3um = data[15] + (data[14] << 8)
                    rawGt0_5um = data[17] + (data[16] << 8)
                    rawGt1_0um = data[19] + (data[18] << 8)
                    rawGt2_5um = data[21] + (data[20] << 8)
                    rawGt5_0um = data[23] + (data[22] << 8)
                    rawGt10_0um = data[25] + (data[24] << 8)
                    # Misc data
                    version = data[26]
                    errorCode = data[27]
                    payloadChecksum = data[29] + (data[28] << 8)
    
                    # Calculate the payload checksum (not including the payload checksum bytes)
                    inputChecksum = 0x42 + 0x4d
                    for x in range(0, 27):
                        inputChecksum = inputChecksum + data[x]
    
                    if inputChecksum != payloadChecksum:
                        logging.WARNING("Checksums don't match! - Skipping data")
    
                    # ACQUIRE HUMI-TEMP 
                    (h,t)=Adafruit_DHT.read(22,24)
                    if (h,t)!=(None, None):
                        (humi,temp)=(h,t)
                        th_samples = th_samples + 1
    
    
                    # AVERAGE PM AND HT (over 5s)
                    if time.time()-gap > min_gap:
                        # Standard particulate values in ug/m3
                        av_concPM1_0_CF1 = round(av_concPM1_0_CF1/samples)
                        av_concPM2_5_CF1 = round(av_concPM2_5_CF1/samples)
                        av_concPM10_0_CF1 = round(av_concPM10_0_CF1/samples)
                        # Atmospheric particulate values in ug/m3
                        av_concPM1_0_ATM = round(av_concPM1_0_ATM/samples) 
                        av_concPM2_5_ATM = round(av_concPM2_5_ATM/samples)
                        av_concPM10_0_ATM = round(av_concPM10_0_ATM/samples)
                        # Raw counts per 0.1l
                        av_rawGt0_3um = round(av_rawGt0_3um/samples)
                        av_rawGt0_5um = round(av_rawGt0_5um/samples)
                        av_rawGt1_0um = round(av_rawGt1_0um/samples)
                        av_rawGt2_5um = round(av_rawGt2_5um/samples)
                        av_rawGt5_0um = round(av_rawGt5_0um/samples)
                        av_rawGt10_0um = round(av_rawGt10_0um/samples)
                        #humi/temp
                        if th_samples:
                            av_temp = round(av_temp/th_samples,1)
                            av_humi = round(av_humi/th_samples,1)
    
                        #displayData()
                        dbPushData()
                        
                        av_concPM1_0_CF1 = 0
                        av_concPM2_5_CF1 = 0
                        av_concPM10_0_CF1 = 0 
                        # Atmospheric partic
                        av_concPM1_0_ATM = 0
                        av_concPM2_5_ATM = 0
                        av_concPM10_0_ATM = 0
                        # Raw counts per 0.1
                        av_rawGt0_3um = 0 
                        av_rawGt0_5um = 0 
                        av_rawGt1_0um = 0 
                        av_rawGt2_5um = 0 
                        av_rawGt5_0um = 0 
                        av_rawGt10_0um = 0
                        #humi/temp
                        av_humi = 0
                        av_temp = 0
                        samples = 0
                        th_samples = 0
                        gap=time.time()
    
    
                    else: # keep summing values
                        now = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
                        # Standard particulate values in ug/m3
                        av_concPM1_0_CF1 = av_concPM1_0_CF1 + concPM1_0_CF1
                        av_concPM2_5_CF1 = av_concPM2_5_CF1 + concPM2_5_CF1
                        av_concPM10_0_CF1 = av_concPM10_0_CF1 + concPM10_0_CF1
                        # Atmospheric particulate values in ug/m3
                        av_concPM1_0_ATM = av_concPM1_0_ATM + concPM1_0_ATM
                        av_concPM2_5_ATM = av_concPM2_5_ATM + concPM2_5_ATM
                        av_concPM10_0_ATM = av_concPM10_0_ATM + concPM10_0_ATM
                        # Raw counts per 0.1l
                        av_rawGt0_3um = av_rawGt0_3um + rawGt0_3um
                        av_rawGt0_5um = av_rawGt0_5um + rawGt0_5um
                        av_rawGt1_0um = av_rawGt1_0um + rawGt1_0um
                        av_rawGt2_5um = av_rawGt2_5um + rawGt2_5um
                        av_rawGt5_0um = av_rawGt5_0um + rawGt5_0um
                        av_rawGt10_0um = av_rawGt10_0um + rawGt10_0um
                        # temp-humi
                        if (t,h) != (None, None):
                            av_temp = av_temp + temp
                            av_humi = av_humi + humi
                        samples = samples + 1
    
    
    
                    time.sleep(0.1)  # Maximum recommended delay (as per data sheet)
    except e:
        dbClose()
        logging.error(str(e))
        raise

    
