import pandas as pd
from math import radians, sin, cos, sqrt, atan2
import datetime as dt

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
  if(start is not pd.NaT and end is not pd.NaT):
    return (end - start).total_seconds() / 3600
  else:
    return 0

def driver_metrics(driverid,cnxn):

    distanceCovered=0
    drivingHours=0
    idleTime=0
    averageSpeed=0
    totalTankFuel=0
    fuelUnloaded=0
    totalJob=0
    jobCompleted=0
    alltime_journey=0
    alltime_drivingTime=0
    alltime_idleTime=0
    alltime_averageSpeed=0

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
       Order By dt.LocationUpdateTime

    ''',cnxn)
    
    driver_profile=pd.read_sql_query(f'''
    Select DriverName,
    ContactNumber,
        LicenceNo FROM Driver Where DriverId='{driverid}'
    ''',cnxn)

    
    driverName=driver_profile.loc[0,"DriverName"] # Driver Name
    driverLicenceNo=driver_profile.loc[0,"LicenceNo"] # Driver LicenceNo
    driverContactNo=driver_profile.loc[0,"ContactNumber"] # Driver ContactNo
    date_format = "%Y-%m-%d %H:%M:%S"
    tripMap=[]
    prev_journey=[]
    currentDeliveredPlanId=0
    
    try:
        if (len(driver_df1)>0):
            driver_df1.sort_values(by="LocationUpdateTime",ascending=False,ignore_index=True,inplace=True)
            alltime_df1=driver_df1.copy()
            alltime_df1.sort_values(by="LocationUpdateTime",ascending=False,ignore_index=True,inplace=True)
            if alltime_df1[alltime_df1["DeliveryTrackerStatusId"]==5]["DeliveryTrackerStatusId"].any():
                end_index=alltime_df1.index.get_loc(alltime_df1[alltime_df1["DeliveryTrackerStatusId"]==5].index[0])
                alltime_df1=alltime_df1[end_index:]
                # alltime_df1=alltime_df1[alltime_df1["DeliveryTrackerStatusId"]!=4]
                alltime_df1.reset_index(inplace=True,drop=True)

                plan_index=alltime_df1[alltime_df1["DeliveryTrackerStatusId"]==1].index
                
                temp_index=0
                
                for i in plan_index:
                    temp_df=alltime_df1[temp_index:i+1]
                    temp_df.sort_values(by="LocationUpdateTime",ascending=True,ignore_index=True,inplace=True)
                    temp_df[["Latitude(t+1)","Longitude(t+1)"]]=temp_df[["Latitude","Longitude"]].shift(periods=1)
                    temp_df["LocationUpdateTime(t+1)"]=temp_df["LocationUpdateTime"].shift(periods=1)
                    temp_df["Distance"]=temp_df.dropna(subset=["Latitude(t+1)","Longitude(t+1)","Latitude","Longitude"], how='any').apply(lambda row:haversine(row["Latitude"],row["Longitude"],row["Latitude(t+1)"],row["Longitude(t+1)"]), axis=1)
                    temp_df["Time"]=temp_df.apply(lambda row:calculate_time_difference(row["LocationUpdateTime(t+1)"],row["LocationUpdateTime"]), axis=1)
                    startPoint=temp_df.loc[0,"HubName"] # Start Point
                    office_list=temp_df[alltime_df1["DeliveryTrackerStatusId"]==2]["OfficeName"].dropna().tolist()
                    res = []
                    [res.append(x) for x in office_list if x not in res]
                    res.insert(0,startPoint)
                    alltime_journey+=temp_df["Distance"].sum()*0.25+temp_df["Distance"].sum()
                    alltime_drivingTime+=temp_df[temp_df["Distance"]!=0]["Time"].sum()
                    alltime_idleTime+=temp_df[temp_df["Distance"]==0]["Time"].sum()
                    prev_distanceCovered=float(temp_df["Distance"].sum()*0.25+temp_df["Distance"].sum())

                    if temp_df.loc[0,"DeliveryPlanTypeId"]==1:
                        res.append(startPoint)

                    driverTrip=alltime_df1[temp_index:i+1]
                    driverTrip=driverTrip[driverTrip["DeliveryTrackerStatusId"]==2]
                    driverTrip["Status"]="Delivered"
                    driverTrip["LocationUpdateTime"]=driverTrip["LocationUpdateTime"].dt.strftime(date_format)
                    # Rename columns
                    driverTrip.rename(columns={
                        "DeliveryPlanId":"deliveryPlanId",
                        "DeliveredQuantity":"Quantity",
                        "OfficeName":"officeName",
                        "LocationUpdateTime":"DeliveredTime",
                        "PlanTitle":"planTitle",
                        "OfficeAddress":"officeAddress"
                    },inplace=True)

                    temp_index=i+1

                
                
                    prev_journey.append(
                        {
                            "planTitle":temp_df.loc[0,"PlanTitle"],
                            "distanceCovered":prev_distanceCovered, # Distance Covered
                            "drivingTime":float(temp_df[temp_df["Distance"]!=0]["Time"].sum()), # Driving Time
                            "idleTime": float(temp_df[temp_df["Distance"]==0]["Time"].sum()), # Idle Time
                            "averageSpeed":float(prev_distanceCovered/temp_df[temp_df["Distance"]!=0]["Time"].sum()) if temp_df[temp_df["Distance"]!=0]["Time"].sum()!=0 else 0, # Average Speed
                            "containerSize":int(temp_df.loc[0,"ContainerSize"]), # Container Size
                            "deliveryPlanId":int(temp_df.loc[0,"DeliveryPlanId"]), # DeliveryPlan
                            "startTime":temp_df.loc[0,"LocationUpdateTime"].strftime(date_format),
                            # "startPoint":temp_df.loc[0,"HubName"], # Start Point
                            "journey":res,
                            "tripMap":driverTrip[["Status","deliveryPlanId","Quantity","officeName","DeliveredTime","planTitle","officeAddress"]][::-1].to_dict('records')
                        }
                    )

            driver_df1=driver_df1[driver_df1["DeliveryPlanId"]==driver_df1.loc[0,"DeliveryPlanId"]]
            alltime_averageSpeed=alltime_journey/alltime_drivingTime if alltime_drivingTime>0 else 0
            
            if driver_df1[driver_df1["DeliveryTrackerStatusId"]==1]["DeliveryTrackerStatusId"].any():
                start_index=driver_df1.index.get_loc(driver_df1[driver_df1["DeliveryTrackerStatusId"]==1].index[0])+1
                driver_df1.sort_values(by="LocationUpdateTime",ascending=True,ignore_index=True,inplace=True)
                driver_df1=driver_df1[-start_index:]
                if driver_df1[driver_df1["DeliveryTrackerStatusId"]!=5]["DeliveryTrackerStatusId"].any():
                    currentDeliveredPlanId=driver_df1.loc[0,"DeliveryPlanId"]
                    driver_df1[["Latitude(t+1)","Longitude(t+1)"]]=driver_df1[["Latitude","Longitude"]].shift(periods=1)
                    driver_df1["LocationUpdateTime(t+1)"]=driver_df1["LocationUpdateTime"].shift(periods=1)
                    driver_df1["Distance"]=driver_df1.dropna(subset=["Latitude(t+1)","Longitude(t+1)","Latitude","Longitude"], how='any').apply(lambda row:haversine(row["Latitude"],row["Longitude"],row["Latitude(t+1)"],row["Longitude(t+1)"]), axis=1)
                    driver_df1["Time"]=driver_df1.apply(lambda row:calculate_time_difference(row["LocationUpdateTime(t+1)"],row["LocationUpdateTime"]), axis=1)

                    driver_df1.reset_index(inplace=True,drop=True)
                    deliveryPlanId=driver_df1.loc[0,"DeliveryPlanId"]

                    deliveryPlanDetails=pd.read_sql_query(f'''
                        SELECT
                        dpd.DeliveredQuantity,
                        dpd.ApprovedQuantity,
                        dpd.PlannedQuantity,
                        dpd.DeliveredAt,
                        dpd.DeliveryPlanId,
                        dpd.ExpectedDeliveryTime,
                        dp.planTitle,
                        o.OfficeId,
                        o.OfficeName,
                        o.officeAddress,
                        o.Latitude As OfficeLatitude,
                        o.Longitude As OfficeLongitude

                        FROM DeliveryPlanDetails dpd
                                                          
                        LEFT JOIN
                        DeliveryPlan dp ON dp.DeliveryPlanId=dpd.DeliveryPlanId

                        LEFT JOIN
                        Office o ON o.OfficeId=dpd.OfficeId
                    
                        Where dpd.DeliveryPlanId={deliveryPlanId} AND
                        dpd.DeliveryPlanDetailsStatusId!=3
                        Order By dpd.DeliveredAt

                        ''',cnxn)
                    
                    current_trip=driver_df1[driver_df1['DeliveryTrackerStatusId']==2]['OfficeId'].drop_duplicates().tolist()
                    planned_trip=deliveryPlanDetails["OfficeId"].tolist()
                    
                    for i in range(len(planned_trip)):
                        if deliveryPlanDetails.loc[i,"OfficeId"] in current_trip:
                            tripMap.append({
                                "Status":"Delivered",
                                "officeAddress":deliveryPlanDetails.loc[i,"officeAddress"],
                                "deliveryPlanId":int(deliveryPlanDetails.loc[i,"DeliveryPlanId"]),
                                "planTitle":deliveryPlanDetails.loc[i,"planTitle"],
                                "officeName":deliveryPlanDetails.loc[i,"OfficeName"],
                                "Quantity":deliveryPlanDetails.loc[i,"DeliveredQuantity"],
                                "approvedQuantity":deliveryPlanDetails.loc[i,"ApprovedQuantity"],
                                "plannedQuantity":deliveryPlanDetails.loc[i,"PlannedQuantity"],
                                "DeliveredTime":deliveryPlanDetails.loc[i,"DeliveredAt"].strftime(date_format),
                                "expectedDeliveryTime":deliveryPlanDetails.loc[i,"ExpectedDeliveryTime"].strftime(date_format),
                            })
                        else:
                            tripMap.append({
                                    "Status":"Pending",
                                    "officeAddress":deliveryPlanDetails.loc[i,"officeAddress"],
                                    "deliveryPlanId":int(deliveryPlanDetails.loc[i,"DeliveryPlanId"]),
                                    "planTitle":deliveryPlanDetails.loc[i,"planTitle"], 
                                    "officeName":deliveryPlanDetails.loc[i,"OfficeName"],
                                    "Quantity":deliveryPlanDetails.loc[i,"ApprovedQuantity"],
                                    "DeliveredTime":None,
                                    "approvedQuantity":deliveryPlanDetails.loc[i,"ApprovedQuantity"],
                                    "plannedQuantity":deliveryPlanDetails.loc[i,"PlannedQuantity"],
                                    "expectedDeliveryTime":deliveryPlanDetails.loc[i,"ExpectedDeliveryTime"].strftime(date_format),
                                })

                    
                    totalTankFuel=deliveryPlanDetails['ApprovedQuantity'].sum() # Total Tank fuel
                    fuelUnloaded=driver_df1[driver_df1['DeliveryTrackerStatusId']==2][['OfficeId','DeliveredQuantity']].drop_duplicates()["DeliveredQuantity"].sum() # Fuel Unloaded

                    totalJob=deliveryPlanDetails['ApprovedQuantity'].count() # Total Job
                    jobCompleted=driver_df1[driver_df1['DeliveryTrackerStatusId']==2][['OfficeId','DeliveredQuantity']].drop_duplicates()["DeliveredQuantity"].count() # Job Completed

                    distanceCovered=driver_df1[driver_df1["Distance"]!=0]["Distance"].sum()*0.25 + driver_df1[driver_df1["Distance"]!=0]["Distance"].sum() # Distance Covered
                    drivingHours=driver_df1[driver_df1["Distance"]!=0]["Time"].sum() # Driving Hours
                    idleTime=driver_df1[driver_df1["Distance"]==0]["Time"].sum() # Idle Time
                    averageSpeed=distanceCovered/drivingHours if drivingHours!=0 else 0 # Average Speed

                    alltime_journey+=distanceCovered
                    alltime_drivingTime+=drivingHours
                    alltime_idleTime+=idleTime
                    alltime_averageSpeed=alltime_journey/alltime_drivingTime if alltime_drivingTime!=0 else 0
    except Exception as e:
        print("Driver Metrics Error", e)

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
        },
        "prevJourney":prev_journey,
        "myTrip":tripMap,
        "currentDeliveredPlanId":int(currentDeliveredPlanId),
        "alltime":{
            "distanceCovered":float(alltime_journey),
            "drivingTime":float(alltime_drivingTime),
            "idleTime":float(alltime_idleTime),
            "averageSpeed":float(alltime_averageSpeed)
        }
    }


