import os
import json
import datetime as dt
import pandas as pd

def convertKMToMiles(inKM):
    '''Function will convert KM to miles''' 
    miles=  inKM * .62137119
    return miles

def convertMetersSecondToMPH(inMS):
    '''Function will convert meters per second to mph''' 
    mph = inMS * 2.23693629
    return mph

def calculateRunType(inRunDict:dict):
    '''Searches the dictionary passed in for the boundingBox value and returns 'Outdoors/Mobile' if the run is 
    found to be an outdoors run, else 'Stationary' is returned.   
    '''
    try:
        inRunDict.get('boundingBox')[0]
        return 'Outdoors/Mobile'
    except:
        return 'Stationary'


def processFile(filePath:str):


    ## compressed file path is passed in.
    filePath = filePath.replace('.zip','.metadata.json')
    
    ### columns list for data frame.
    columns = [
    'runType',
    'distance_Miles',
    'outStartDate',
    'outStartTime',
    'distance_KM',
    'timeZone',
    'duration_Seconds',
    'avgSpeed_MetersSec',
    'avgSpeed_MPH',
    'maxSpeed_MetersSec',
    'maxSpeed_MPH',
    'calories',
    'avgHR',
    'maxHR',
    'elevationGain',
    'elevationLoss',
    'minElevation',
    'maxElevation',
    'avgCadence',
    'maxCadence',
    'steps',
    'startLatitude',
    'startLongitude'    
    ]


    ## blank df which will hold runs.
    dfRuns = pd.DataFrame(columns=columns)
    
    ##init vars used for appending to df 
    runType,distance_Miles,outStartDate,outStartTime,distance_KM,timeZone,duration_Seconds,avgSpeed_MetersSec, \
    avgSpeed_MPH,maxSpeed_MetersSec,maxSpeed_MPH,calories,avgHR,maxHR,elevationGain,elevationLoss,minElevation, \
    maxElevation,avgCadence,maxCadence,steps,startLatitude,startLongitude = "","","","","","","","","","","","","","","","","","","","","","",""

    print("###### Beginning processing of downloaded, extracted files...")
    
    
    ### limit to metadata files, their detail is suffice
    if filePath.endswith('metadata.json'):
        print("Reading... ",filePath)
        
        ## read the entire json string of the file into a dictionary
        with open(filePath,'r') as string:
            file_dict=json.load(string)
        string.close()       


        ##reset vars used for appending to df (23)
        runType,distance_Miles,outStartDate,outStartTime,distance_KM,timeZone,duration_Seconds,avgSpeed_MetersSec, \
        avgSpeed_MPH,maxSpeed_MetersSec,maxSpeed_MPH,calories,avgHR,maxHR,elevationGain,elevationLoss,minElevation, \
        maxElevation,avgCadence,maxCadence,steps,startLatitude,startLongitude= "","","","","","","","","","","","","","","","","","","","","","",""

        runType = calculateRunType(file_dict)

        for key in file_dict:          
            # DATE, TIME, TIMEZONE, DURATION, DISTANCE, AVG SPEED, MAX SPEED, CALORIES, AVG HR, MAX HR, ELE GAIN, ELE LOSS, MIN ELE, MAX ELE
            #AVGCADENCE, MAXCADENCE, STEPS,
            #            
            if key == 'distance':                
                distance_KM = str(file_dict[key])
                distance_KM = float(distance_KM) / 1000
                distance_Miles = convertKMToMiles(distance_KM)

            elif key == 'startTime':
                startTime = file_dict[key]['time'] 
                            
                startTime = startTime.replace('Z','').replace('T',' ') ##format the value for constructing date time from pandas...
                
                startDateTimeObj = pd.to_datetime(startTime) ##cast to datetime using Pandas               
                outStartDate = startDateTimeObj.strftime("%Y-%m-%d")                          
                outStartTime = startDateTimeObj.time()                      

                ##try to get timezone
                try:
                    timeZone = file_dict[key]['timeZone']   
                except:
                    timeZone = 'unknown'

            elif key == 'duration':
                duration_Seconds = file_dict[key]

            elif key == 'avgSpeed':
                avgSpeed_MetersSec = file_dict[key]
                avgSpeed_MPH = convertMetersSecondToMPH(avgSpeed_MetersSec)

            elif key == 'maxSpeed':
                maxSpeed_MetersSec = file_dict[key]
                maxSpeed_MPH = convertMetersSecondToMPH(maxSpeed_MetersSec)

            elif key == 'calories':
                calories = file_dict[key]                

            elif key == 'avgHeartrate':
                avgHR = file_dict[key]            

            elif key == 'maxHeartrate':
                maxHR = file_dict[key]                

            elif key == 'elevationGain':
                elevationGain = file_dict[key]                

            elif key == 'elevationLoss':
                elevationLoss = file_dict[key]            

            elif key == 'minElevation':
                minElevation = file_dict[key]
            
            elif key == 'maxElevation':
                maxElevation = file_dict[key]
            
            elif key == 'avgCadence':
                avgCadence = file_dict[key]

            elif key == 'maxCadence':
                maxCadence = file_dict[key]

            elif key == 'steps':
                steps = file_dict[key]
            
            elif key == 'displayPath':
                ##run type should be Outdoors/Mobile
                startLatitude = file_dict[key][0]['lat'] 
                startLongitude = file_dict[key][0]['lon']               

            
            if startLatitude =='':
                startLatitude = 0

            if startLongitude =='':
                startLongitude = 0


            row = pd.Series([runType,distance_Miles,outStartDate,outStartTime,distance_KM,timeZone,
            duration_Seconds,avgSpeed_MetersSec,avgSpeed_MPH,maxSpeed_MetersSec,maxSpeed_MPH
            ,calories,avgHR,maxHR,elevationGain,elevationLoss,minElevation,maxElevation,
            avgCadence,maxCadence,steps,startLatitude,startLongitude],index=dfRuns.columns)            
            
            
            #append row to dataframe at counters index
            dfRuns.loc[0]=row

            
            

        ##return the df   
    return dfRuns

