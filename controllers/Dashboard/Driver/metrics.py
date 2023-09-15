import pandas as pd
from math import radians, sin, cos, sqrt, atan2

def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in kilometers

    # Convert latitude and longitude to radians
    lat1, lon1, lat2, lon2 = map(radians, [float(lat1), float(lon1), float(lat2), float(lon2)])

    # Calculate the differences between the latitudes and longitudes
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    # Apply the Haversine formula
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = R * c

    return distance

def calculate_time_difference(start, end):
    return (end - start).total_seconds() / 3600

def driver_metrics(driverid,cnxn):

    distanceCovered=0
    drivingHours=0
    idleTime=0
    averageSpeed=0
    totalTankFuel=0
    fuelUnloaded=0
    totalJob=0
    jobCompleted=0

    driver_df1=pd.read_sql_query(f'''
    SELECT
      dt.DeliveryTrackerStatusId,
      dt.DeliveryPlanId,
      dt.LocationUpdateTime,
      dt.Latitude,
      dt.Longitude,
      dt.DeliveryPlanDetailsId,
      dp.PlanTitle,
      dp.DeliveryLimit,
      dp.ContainerSize,
      dp.ProductId,
      dp.StartPointId,
      dp.DeliveryPlanTypeId,
      h.HubName,
      h.Latitude AS HubLatitude,
      h.Longitude AS HubLongitude,
      dpd.OfficeId,
      dpd.ApprovedQuantity,
      dpd.DeliveredQuantity,
      o.OfficeName,
      o.OfficeAddress,
      o.Latitude As OfficeLatitude,
      o.Longitude As OfficeLongitude

      FROM DeliveryTracker dt
      LEFT JOIN
      DeliveryPlan dp ON dt.deliveryPlanId=dp.deliveryPlanId
      LEFT JOIN
      Hub h ON h.HubId=dp.StartPointId
      LEFT JOIN
      DeliveryPlanDetails dpd ON dpd.DeliveryPlanDetailsId=dt.DeliveryPlanDetailsId AND dpd.DeliveryPlanId=dt.DeliveryPlanId
      LEFT JOIN
      Office o ON o.OfficeId=dpd.OfficeId

       Where dt.DriverId='{driverid}' AND dp.IsDeleted=0

    ''',cnxn)
    
    driver_profile=pd.read_sql_query(f'''
    Select DriverName,
    ContactNumber,
        LicenceNo FROM Driver Where DriverId='{driverid}'
    ''',cnxn)

    
    driverName=driver_profile.loc[0,"DriverName"] # Driver Name
    driverLicenceNo=driver_profile.loc[0,"LicenceNo"] # Driver LicenceNo
    driverContactNo=driver_profile.loc[0,"ContactNumber"] # Driver ContactNo
    
    try:
        if (len(driver_df1)>0):
            driver_df1.sort_values(by="LocationUpdateTime",ascending=False,ignore_index=True,inplace=True)
            driver_df1=driver_df1[driver_df1["DeliveryPlanId"]==driver_df1.loc[0,"DeliveryPlanId"]]
            
            if driver_df1[driver_df1["DeliveryTrackerStatusId"]==1]["DeliveryTrackerStatusId"].any():
                start_index=driver_df1.index.get_loc(driver_df1[driver_df1["DeliveryTrackerStatusId"]==1].index[0])+1
                driver_df1.sort_values(by="LocationUpdateTime",ascending=True,ignore_index=True,inplace=True)
                driver_df1=driver_df1[-start_index:]
                if driver_df1[driver_df1["DeliveryTrackerStatusId"]!=5]["DeliveryTrackerStatusId"].any():
                    driver_df1[["Latitude(t+1)","Longitude(t+1)"]]=driver_df1[["Latitude","Longitude"]].shift(periods=1)
                    driver_df1["LocationUpdateTime(t+1)"]=driver_df1["LocationUpdateTime"].shift(periods=1)
                    driver_df1["Distance"]=driver_df1.dropna(subset=["Latitude(t+1)","Longitude(t+1)","Latitude","Longitude"], how='any').apply(lambda row:haversine(row["Latitude"],row["Longitude"],row["Latitude(t+1)"],row["Longitude(t+1)"]), axis=1)
                    driver_df1["Time"]=driver_df1.dropna(subset=["LocationUpdateTime(t+1)","LocationUpdateTime"], how='any').apply(lambda row:calculate_time_difference(row["LocationUpdateTime(t+1)"],row["LocationUpdateTime"]), axis=1)

                    driver_df1.reset_index(inplace=True,drop=True)
                    deliveryPlanId=driver_df1.loc[0,"DeliveryPlanId"]

                    deliveryPlanDetails=pd.read_sql_query(f'''
                        SELECT
                        dpd.DeliveredQuantity,
                        dpd.ApprovedQuantity,
                        o.OfficeName,
                        o.OfficeAddress,
                        o.Latitude As OfficeLatitude,
                        o.Longitude As OfficeLongitude

                        FROM DeliveryPlanDetails dpd

                        LEFT JOIN
                        Office o ON o.OfficeId=dpd.OfficeId
                    
                        Where dpd.DeliveryPlanId={deliveryPlanId}

                        ''',cnxn)
                    
                    totalTankFuel=deliveryPlanDetails['ApprovedQuantity'].sum() # Total Tank fuel
                    fuelUnloaded=driver_df1[driver_df1['DeliveryTrackerStatusId']==2][['OfficeId','DeliveredQuantity']].drop_duplicates()["DeliveredQuantity"].sum() # Fuel Unloaded

                    totalJob=deliveryPlanDetails['ApprovedQuantity'].count() # Total Job
                    jobCompleted=driver_df1[driver_df1['DeliveryTrackerStatusId']==2][['OfficeId','DeliveredQuantity']].drop_duplicates()["DeliveredQuantity"].count() # Job Completed

                    distanceCovered=driver_df1[driver_df1["Distance"]!=0]["Distance"].sum() # Distance Covered
                    drivingHours=driver_df1[driver_df1["Distance"]!=0]["Time"].sum() # Driving Hours
                    idleTime=driver_df1[driver_df1["Distance"]==0]["Time"].sum() # Idle Time
                    averageSpeed=distanceCovered/drivingHours # Average Speed
    except:
        print("Driver Metrics Error")

    return {
        "profile":{
            "driverName":driverName,
            "driverLicenceNo":driverLicenceNo,
            "driverContactNo":driverContactNo
        },
        "distanceCovered":float(distanceCovered),
        "drivingTime":float(drivingHours),
        "idleTime":float(idleTime),
        "averageSpeed":float(averageSpeed),
        "graph1":{
            "totalTankFuel":float(totalTankFuel),
            "fuelUnloaded":float(fuelUnloaded)
        },
        "graph2":{
            "totalJob":int(totalJob),
            "jobCompleted":int(jobCompleted)
        }
    }


