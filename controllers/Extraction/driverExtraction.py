import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from math import radians, sin, cos, sqrt, atan2

def RScore(x,p,d):
    if x <= d[p][0.25]:
        return 1
    elif x <= d[p][0.50]:
        return 2
    elif x <= d[p][0.75]:
        return 3
    else:
        return 4

def FMScore(x,p,d):
    if x <= d[p][0.25]:
        return 4
    elif x <= d[p][0.50]:
        return 3
    elif x <= d[p][0.75]:
        return 2
    else:
        return 1

def driverRecommendation(cnxn,DeliveryPlanId):
    driver_df=pd.read_sql_query('Select dp.deliveryPlanId,dp.driverId,dpd.officeId,dp.deliveryPlanStatusId,dp.expectedDeliveryDate,dp.expectedReturnTime,dp.actualReturnTime from DeliveryPlan dp \
Left Join DeliveryPlanDetails dpd on dp.deliveryPlanId=dpd.deliveryPlanId \
Where dp.isDeleted=0',cnxn)
    segmented_rfm=pd.DataFrame()
    if len(driver_df)>0:
        office_list=driver_df[driver_df["deliveryPlanId"]==DeliveryPlanId]["officeId"].to_list()

        time_difference=driver_df[['deliveryPlanId','driverId','expectedReturnTime','actualReturnTime']].drop_duplicates().reset_index(drop=True)
        time_difference[['expectedReturnTime','actualReturnTime']]=time_difference[['expectedReturnTime','actualReturnTime']].apply(pd.to_datetime)
        # time_difference['expectedReturnTime']=time_difference['expectedReturnTime'].values.astype(int)/ 10**9
        # time_difference['actualReturnTime']=time_difference['actualReturnTime'].values.astype(int)/ 10**9
        # time_difference[['expectedReturnTime','actualReturnTime']].fillna(0,inplace=True)
        
        
        time_difference['T_difference']=time_difference['expectedReturnTime']-time_difference['actualReturnTime']
        time_difference['T_difference'].replace({pd.NaT:0},inplace=True)
        time_difference['T_difference']=time_difference['T_difference'].values.astype(int)/ 10**9
        time_difference=time_difference.groupby('driverId').agg({'T_difference':lambda x:x.sum()}).reset_index()

        officeCount=driver_df[(driver_df["deliveryPlanStatusId"]>3) & (driver_df["deliveryPlanStatusId"]<7)].groupby('driverId').agg({'officeId':lambda x :len([i for i in x if i in office_list])}).reset_index()

        rfmTable=pd.merge(officeCount,time_difference,on='driverId',how='outer')

        quantiles = rfmTable.quantile(q=[0.25,0.5,0.75])
        quantiles = quantiles.to_dict()

        segmented_rfm = rfmTable
        segmented_rfm['office_count'] = segmented_rfm['officeId'].apply(RScore, args=('officeId',quantiles,))
        segmented_rfm['on_time'] = segmented_rfm['T_difference'].apply(FMScore, args=('T_difference',quantiles))
        segmented_rfm['RFMScore'] = segmented_rfm.office_count.map(str) + segmented_rfm.on_time.map(str)
        segmented_rfm['RFMScore']=segmented_rfm['RFMScore'].values.astype(int)
        segmented_rfm['recommendation']=True

    return segmented_rfm
    
def ExtractingDriverStatus(DeliveryPlanId,cnxn):
    driver_df=pd.read_sql_query(f'''
                                Select 
                                d.driverName,d.licenceNo,d.contactNumber,d.driverId
                                from  Driver d 
                                
                                Where d.IsActive=1
                                ''',cnxn)
    
    delivery_df=pd.read_sql_query(f'''
                                Select 
                                dp.driverId,dp.deliveryPlanId,dp.expectedDeliveryDate,dp.expectedReturnTime,dp.planDate,dp.DeliveryPlanStatusId
                                from DeliveryPlan dp
                                
                                Where dp.DeliveryPlanId={DeliveryPlanId}
                                ''',cnxn)
    

    date_format = "%Y-%m-%d %H:%M:%S"
    date_format2 = "%Y-%m-%dT%H:%M:%S"

    driver_assigned_df=pd.read_sql_query(f'''
                                Select dp.driverId,dp.deliveryPlanId,dp.expectedDeliveryDate,dp.planDate,dp.DeliveryPlanStatusId,dps.DeliveryPlanStatus,
                                d.driverName,d.licenceNo,d.contactNumber 
                                from DeliveryPlan dp
                                         LEFT JOIN DeliveryPlanStatusMaster dps ON dps.DeliveryPlanStatusId=dp.DeliveryPlanStatusId
                                Left Join Driver d ON d.DriverId=dp.DriverId
                                Where dp.IsDeleted=0 AND d.IsActive=1 AND dp.deliveryPlanStatusId!=7 AND
                                         dp.expectedReturnTime >='{datetime.strftime(delivery_df['expectedDeliveryDate'].iloc[0],date_format)}' AND (dp.expectedReturnTime <='{datetime.strftime(delivery_df['expectedReturnTime'].iloc[0]+timedelta(seconds=1),date_format)}' OR dp.actualReturnTime<='{datetime.strftime(delivery_df['expectedReturnTime'].iloc[0]+timedelta(seconds=1),date_format)}') AND
                                         dp.expectedDeliveryDate >='{datetime.strftime(delivery_df['expectedDeliveryDate'].iloc[0],date_format)}' AND (dp.expectedDeliveryDate <='{datetime.strftime(delivery_df['expectedReturnTime'].iloc[0]+timedelta(seconds=1),date_format)}' )

                                ORDER BY dp.expectedDeliveryDate Desc;''',cnxn)
    if len(driver_assigned_df)>0:
        driver_assigned_df["assigned"]=True
        driver_assigned_df[["planDate","expectedDeliveryDate"]]=driver_assigned_df[["planDate","expectedDeliveryDate"]].apply(pd.to_datetime)
        driver_assigned_df["planDate"]=driver_assigned_df["planDate"].dt.strftime(date_format2)
        driver_assigned_df["expectedDeliveryDate"]=driver_assigned_df["expectedDeliveryDate"].dt.strftime(date_format2)
        driver_assigned_df.set_index('deliveryPlanId',inplace=True)
        order_list=list(driver_assigned_df.index)
        # LOGIC to remove 2 or more delivery plan with same driver at same time
        if DeliveryPlanId in order_list:
            order_list.remove(DeliveryPlanId)
            order_list.insert(0,DeliveryPlanId)
            driver_assigned_df=driver_assigned_df.reindex(order_list)
        driver_assigned_df.reset_index(inplace=True)
   
        check_duplicate=driver_assigned_df.duplicated(subset=['driverId']).any()
        if check_duplicate:
            driver_assigned_df=driver_assigned_df.drop_duplicates(subset=['driverId'], keep='first')
    
    
    driver_not_assigned_df=driver_df[~driver_df['driverId'].isin(driver_assigned_df['driverId'])]
    driver_not_assigned_df["assigned"]=False
    recommended_df=driverRecommendation(cnxn,DeliveryPlanId)
    if(len(recommended_df)>0):
        driver_not_assigned_df=driver_not_assigned_df.merge(recommended_df,how='left',on='driverId')
        driver_status=pd.concat([driver_assigned_df,driver_not_assigned_df])
        driver_status.replace({np.nan: None}, inplace = True)
        driver_status.sort_values(by=['RFMScore'],inplace=True,ascending=False)
        driver_status.reset_index(drop=True,inplace=True)
        driver_status.loc[2:,'recommendation']=False

    else:
        driver_status=pd.concat([driver_assigned_df,driver_not_assigned_df])
        driver_status.replace({np.nan: None}, inplace = True)
    
    return driver_status.to_dict('records'),delivery_df.to_dict('records')

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
                # alltime_journey+=temp_df["Distance"].sum()
                # alltime_drivingTime+=temp_df[temp_df["Distance"]!=0]["Time"].sum()
                # alltime_idleTime+=temp_df[temp_df["Distance"]==0]["Time"].sum()
                distanceCovered=float(temp_df["Distance"].sum()*0.25+temp_df["Distance"].sum()) # Distance Covered

                if temp_df.loc[0,"DeliveryPlanTypeId"]==1:
                    res.append(startPoint)

                temp_index=i+1
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
