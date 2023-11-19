import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from math import radians, sin, cos, sqrt, atan2

def ExtractingDriverStatus(DeliveryPlanId,cnxn):
    driver_df=pd.read_sql_query(f'''
                                Select 
                                d.driverName,d.licenceNo,d.contactNumber,d.driverId
                                from  Driver d 
                                Where d.IsActive=1
                                ''',cnxn)
    
    delivery_df=pd.read_sql_query(f'''
                                Select 
                                dp.driverId,dp.deliveryPlanId,dp.expectedDeliveryDate,dp.planDate
                                from DeliveryPlan dp
                                Where dp.DeliveryPlanId={DeliveryPlanId}
                                ''',cnxn)
    
    date_format = "%Y-%m-%d %H:%M:%S"
    date_format2 = "%Y-%m-%dT%H:%M:%S"
 

    driver_assigned_df=pd.read_sql_query(f'''
                                Select dp.driverId,dp.deliveryPlanId,dp.expectedDeliveryDate,dp.planDate,
                                d.driverName,d.licenceNo,d.contactNumber 
                                from DeliveryPlan dp
                                Left Join Driver d ON d.DriverId=dp.DriverId
                                Where dp.IsDeleted=0 AND d.IsActive=1 AND 
                                         dp.expectedDeliveryDate >='{datetime.strftime(delivery_df['planDate'].iloc[0],date_format)}' AND (dp.expectedDeliveryDate <='{datetime.strftime(delivery_df['expectedDeliveryDate'].iloc[0]+timedelta(seconds=1),date_format)}' OR dp.actualReturnTime<='{datetime.strftime(delivery_df['expectedDeliveryDate'].iloc[0]+timedelta(seconds=1),date_format)}') AND
                                         dp.planDate >='{datetime.strftime(delivery_df['planDate'].iloc[0],date_format)}' AND (dp.planDate <='{datetime.strftime(delivery_df['expectedDeliveryDate'].iloc[0]+timedelta(seconds=1),date_format)}' )

                                ORDER BY dp.expectedDeliveryDate Desc;''',cnxn)
    driver_assigned_df["assigned"]=True
    driver_assigned_df[["planDate","expectedDeliveryDate"]]=driver_assigned_df[["planDate","expectedDeliveryDate"]].apply(pd.to_datetime)
    driver_assigned_df["planDate"]=driver_assigned_df["planDate"].dt.strftime(date_format2)
    driver_assigned_df["expectedDeliveryDate"]=driver_assigned_df["expectedDeliveryDate"].dt.strftime(date_format2)
    
    
    driver_not_assigned_df=driver_df[~driver_df['driverId'].isin(driver_assigned_df['driverId'])]
    driver_not_assigned_df["assigned"]=False

    driver_status=pd.concat([driver_assigned_df,driver_not_assigned_df])
    driver_status.replace({np.nan: None}, inplace = True)
    
    return driver_status.to_dict('records')

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
  
def ExtractingDriverHistory(driverid,cnxn):
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
        
    
    date_format = "%Y-%m-%dT%H:%M:%S"
    prev_journey=[]
    
    try:
        if (len(driver_df1)>0):
            # driver_df1.sort_values(by="LocationUpdateTime",ascending=False,ignore_index=True,inplace=True)
            alltime_df1=driver_df1.copy()
            alltime_df1.sort_values(by="LocationUpdateTime",ascending=False,ignore_index=True,inplace=True)
            end_index=alltime_df1.index.get_loc(alltime_df1[alltime_df1["DeliveryTrackerStatusId"]==5].index[0])
            alltime_df1=alltime_df1[end_index:]
            # alltime_df1=alltime_df1[alltime_df1["DeliveryTrackerStatusId"]!=4]
            alltime_df1.reset_index(inplace=True,drop=True)

            plan_index=alltime_df1[alltime_df1["DeliveryTrackerStatusId"]==1].index
            for i in plan_index:
                temp_df=alltime_df1[:i+1]
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
                # alltime_journey+=temp_df["Distance"].sum()
                # alltime_drivingTime+=temp_df[temp_df["Distance"]!=0]["Time"].sum()
                # alltime_idleTime+=temp_df[temp_df["Distance"]==0]["Time"].sum()
                distanceCovered=float(temp_df["Distance"].sum()*0.25+temp_df["Distance"].sum()) # Distance Covered

                if temp_df.loc[0,"DeliveryPlanTypeId"]==1:
                    res.append(startPoint)
                prev_journey.append(
                    {
                        "distanceCovered":distanceCovered, # Distance Covered
                        "drivingTime":float(temp_df[temp_df["Distance"]!=0]["Time"].sum()), # Driving Time
                        "idleTime": float(temp_df[temp_df["Distance"]==0]["Time"].sum()), # Idle Time
                        "averageSpeed":float(distanceCovered/temp_df[temp_df["Distance"]!=0]["Time"].sum()) if temp_df[temp_df["Distance"]!=0]["Time"].sum()!=0 else 0, # Average Speed
                        "containerSize":int(temp_df.loc[0,"ContainerSize"]), # Container Size
                        "deliveryPlanId":int(temp_df.loc[0,"DeliveryPlanId"]), # DeliveryPlan
                        "startTime":temp_df.loc[0,"LocationUpdateTime"].strftime(date_format),
                        # "startPoint":temp_df.loc[0,"HubName"], # Start Point
                        "journey":res
                    }
                )
                
    except Exception as e:
        print("Error at ExtractionDriverHistory ",e)

    return prev_journey
