import pandas as pd
import numpy as np
from controllers.Extraction.extraction import Extracting
import warnings
warnings.filterwarnings('ignore')

def ExtractingFromDeliveryPlan(DeliveryPlanId,cnxn,No_of_days_for_delivery):

    df=pd.read_sql_query(f'''
                         SELECT
   df.OfficeId,
    df.OfficeName,
   df.Longitude,
    df.Latitude,
    
	d.ContainerSize,
	d.StartPointID,
	d.StartLatitude,
	d.StartLongitude,
	d.DeliveryPlanId,
    m.masterOfficeId,
    m.OfficeName As masterOfficeName,
    d.HubName,
    d.PlannedQuantity,d.CurrentQuantity,d.AvailableQuantity,d.ProductId,d.DeliveryLimit,
                         d.PlanDate,d.ExpectedDeliveryDate,d.DeliveryPlanStatusId,d.CreatedBy,d.UpdatedBy,d.CreatedOn,d.UpdatedOn,
                         d.DeliveryPlanDetailsId,d.SequenceNo,d.AdminId,d.ReceivedQuantity,d.DeliveryPlanDetailsStatusId,d.ApprovedQuantity,d.DeliveredQuantity,d.DeliveredAt
FROM
    Office df
    LEFT JOIN
       Office m ON m.OfficeId = df.masterOfficeId

    Inner JOIN(
    Select dpd.DeliveryPlanId,dpd.OfficeId,dpd.PlannedQuantity,dpd.CurrentQuantity,dpd.AvailableQuantity,dpd.DeliveryPlanDetailsStatusId,
    dp.StartPointId,dp.ContainerSize,M.Latitude As StartLatitude,
    M.Longitude As StartLongitude,M.HubName,dp.ProductId,dp.DeliveryLimit,
                         dp.PlanDate,dp.ExpectedDeliveryDate,dp.DeliveryPlanStatusId,dp.CreatedBy,dp.UpdatedBy,dp.CreatedOn,dp.UpdatedOn,
                         dpd.DeliveryPlanDetailsId,dpd.AdminId,dpd.SequenceNo,dpd.ReceivedQuantity,dpd.ApprovedQuantity,dpd.DeliveredQuantity,dpd.DeliveredAt

    from DeliveryPlanDetails dpd

    left join
    DeliveryPlan dp

    on dpd.DeliveryPlanId=dp.DeliveryPlanId

    left join
    Hub M

    on dp.StartPointId=M.HubId
	
    Where 
    dp.DeliveryPlanId={DeliveryPlanId} And
    (dpd.DeliveryPlanDetailsStatusId!=3)
        )As d
    On d.OfficeId=df.OfficeId  
;
                         ''',cnxn)
    df.rename(
            columns={
                "OfficeId": "officeId",
                "OfficeName": "officeName",
                "Longitude": "longitude",
                "Latitude": "latitude",
                "CurrentQuantity": "currentStock",
                "AvailableQuantity": "availableQuantity",
                "PlannedQuantity": "atDeliveryRequirement"
            },
            inplace=True,
        )
  # if totalCapacity value is 0 then replace it to 2000
    df["totalCapacity"]=df["availableQuantity"]+df["currentStock"]
    if (len(df)>0):
        df[["currentStock","totalCapacity"]].fillna(0,inplace=True)
        df["totalCapacity"].replace(to_replace = 0,value = 2000,inplace=True)
        

        df[["latitude","longitude"]].dropna(inplace=True)
        df.reset_index(inplace=True,drop=True)

        Starting_PointId = df["StartPointID"][0]
        Starting_PointName = df["HubName"][0]
        Starting_Point_latitude = df["StartLatitude"][0]
        Starting_Point_longitude = df["StartLongitude"][0]
        total_requirement=df['atDeliveryRequirement'].dropna().sum()
        excess_capacity= df["ContainerSize"][0]-total_requirement
        product_id=df["ProductId"][0]
        minimum_multiple=df["DeliveryLimit"][0]
        PlanDate=df["PlanDate"][0]
        ExpectedDeliveryDate=df["ExpectedDeliveryDate"][0]
        DeliveryPlanStatusId=df["DeliveryPlanStatusId"][0]
        CreatedBy=df["CreatedBy"][0]
        UpdatedBy=df["UpdatedBy"][0]
        CreatedOn=df["CreatedOn"][0]
        UpdatedOn=df["UpdatedOn"][0]
        # Converting the datetime object into string
        PlanDate=PlanDate.strftime("%Y-%m-%d %H:%M:%S") if PlanDate is not None else None
        ExpectedDeliveryDate=ExpectedDeliveryDate.strftime("%Y-%m-%d %H:%M:%S") if ExpectedDeliveryDate is not None else None
        CreatedOn=CreatedOn.strftime("%Y-%m-%d") if CreatedOn is not None else None 
        UpdatedOn=UpdatedOn.strftime("%Y-%m-%d") if UpdatedOn is not None else None

        No_of_days_for_delivery=0 if No_of_days_for_delivery is None else No_of_days_for_delivery

        Unselected_df=Extracting( product_id,cnxn)
        office_list=df["officeId"].to_list()
        Unselected_df=Unselected_df[~Unselected_df["officeId"].isin(office_list)]
    
        Unselected_df["atDeliveryRequirement"]=Unselected_df["totalCapacity"]-Unselected_df["currentStock"]+Unselected_df["avgSales"]*No_of_days_for_delivery 
        Unselected_df.reset_index(inplace=True)
        for i in range(len(Unselected_df)):
            if Unselected_df.loc[i,"atDeliveryRequirement"]>Unselected_df.loc[i,"totalCapacity"]:
                Unselected_df.loc[i,"atDeliveryRequirement"]=Unselected_df.loc[i,"totalCapacity"]
            else:
                Unselected_df.loc[i,"atDeliveryRequirement"]=Unselected_df.loc[i,"atDeliveryRequirement"]
        # Unselected_df["atDeliveryRequirement"] = Unselected_df.apply(lambda row: row["totalCapacity"] if row["atDeliveryRequirement"] > row["totalCapacity"] else row["atDeliveryRequirement"], axis=1)
        Unselected_df["requirement%"]=Unselected_df["atDeliveryRequirement"]/Unselected_df["totalCapacity"]*100
        Unselected_df["requirement%"].fillna(0,inplace=True)
        Unselected_df["atDeliveryRequirement"]= (Unselected_df["atDeliveryRequirement"]//minimum_multiple)*minimum_multiple
        # Unselected_df["currentStock"]=Unselected_df["currentStock"]-Unselected_df["avgSales"]*No_of_days_for_delivery
        Unselected_df["availableQuantity"]=Unselected_df["totalCapacity"]-(Unselected_df["currentStock"]-Unselected_df["avgSales"]*No_of_days_for_delivery)
        Unselected_df["atDeliveryRequirement"].replace(to_replace=0, value=minimum_multiple, inplace=True)
                                        
        
        Unselected_df.sort_values(by="requirement%",inplace=True,ascending=False)
        Unselected_df.reset_index(drop=True,inplace=True)

        return df,Starting_PointId,Starting_PointName,Starting_Point_latitude,Starting_Point_longitude,total_requirement,excess_capacity,Unselected_df[["officeName","latitude","longitude","atDeliveryRequirement","officeId","totalCapacity","currentStock","availableQuantity","masterOfficeId","masterOfficeName"]].to_dict(orient="records"),minimum_multiple,PlanDate,ExpectedDeliveryDate,int(DeliveryPlanStatusId),CreatedBy,UpdatedBy,CreatedOn,UpdatedOn
    else:
        return df,0,0,0,0,0,0,[],None,None,None,None,None,None,None,None
    
def ExtractingDriverRouteFromDeliveryPlan(DeliveryPlanId,cnxn):
    Delivery_df=pd.read_sql_query(f'''
                         SELECT
        df.officeId,
            df.officeName,
        df.longitude,
            df.latitude,
            
            d.containerSize,
            d.startPointID,
            d.startLatitude,
            d.startLongitude,
            d.deliveryPlanId,
            m.masterOfficeId,
            m.OfficeName As masterOfficeName,
            m.officeaddress,
            d.hubName,
            d.plannedQuantity,d.currentQuantity,d.availableQuantity,d.productId,d.deliveryLimit,
                                d.planDate,d.expectedDeliveryDate,d.deliveryPlanStatusId,d.createdBy,d.updatedBy,d.createdOn,d.updatedOn,
                                d.deliveryPlanDetailsId,d.sequenceNo,d.adminId,d.receivedQuantity,d.deliveryPlanDetailsStatusId,d.approvedQuantity,d.deliveredQuantity,d.deliveredAt
        FROM
            Office df
            LEFT JOIN
            Office m ON m.OfficeId = df.masterOfficeId

            Inner JOIN(
            Select dpd.DeliveryPlanId,dpd.OfficeId,dpd.PlannedQuantity,dpd.CurrentQuantity,dpd.AvailableQuantity,dpd.DeliveryPlanDetailsStatusId,
            dp.StartPointId,dp.ContainerSize,M.Latitude As StartLatitude,
            M.Longitude As StartLongitude,M.HubName,dp.ProductId,dp.DeliveryLimit,
                                dp.PlanDate,dp.ExpectedDeliveryDate,dp.DeliveryPlanStatusId,dp.CreatedBy,dp.UpdatedBy,dp.CreatedOn,dp.UpdatedOn,
                                dpd.DeliveryPlanDetailsId,dpd.AdminId,dpd.SequenceNo,dpd.ReceivedQuantity,dpd.ApprovedQuantity,dpd.DeliveredQuantity,dpd.DeliveredAt

            from DeliveryPlanDetails dpd

            left join
            DeliveryPlan dp

            on dpd.DeliveryPlanId=dp.DeliveryPlanId

            left join
            Hub M

            on dp.StartPointId=M.HubId
            
            Where 
            dp.DeliveryPlanId={DeliveryPlanId} And
            (dpd.DeliveryPlanDetailsStatusId!=3)
                )As d
            On d.OfficeId=df.OfficeId  
           ORDER BY d.sequenceNo
        ;
                         ''',cnxn)
    
    date_format="%Y-%m-%dT%H:%M:%S"

    if len(Delivery_df)>0:
        Delivery_df[["createdOn","updatedOn","planDate","expectedDeliveryDate","deliveredAt"]]=Delivery_df[["createdOn","updatedOn","planDate","expectedDeliveryDate","deliveredAt"]].apply(pd.to_datetime)
        Delivery_df["createdOn"]=Delivery_df["createdOn"].dt.strftime(date_format)
        Delivery_df["updatedOn"]=Delivery_df["updatedOn"].dt.strftime(date_format)
        Delivery_df["planDate"]=Delivery_df["planDate"].dt.strftime(date_format)
        Delivery_df["expectedDeliveryDate"]=Delivery_df["expectedDeliveryDate"].dt.strftime(date_format)
        Delivery_df["deliveredAt"]=Delivery_df["deliveredAt"].dt.strftime(date_format)
        Delivery_df.replace({np.nan: None}, inplace = True)
    
    driver_df=pd.read_sql_query(f'''SELECT
        name as status,
        DeliveryTracker.latitude,
        DeliveryTracker.longitude,
        driverId,
        locationUpdateTime,
        DeliveryTracker.deliveryTrackerStatusId,
        DeliveryPlanDetails.officeId,
        Office.officeName
    FROM
        DeliveryTracker
    Left Join DeliveryTrackerStatusMaster ON DeliveryTrackerStatusMaster.DeliveryTrackerStatusId = DeliveryTracker.DeliveryTrackerStatusId
    Left Join DeliveryPlanDetails ON DeliveryPlanDetails.DeliveryPlanDetailsId = DeliveryTracker.DeliveryPlanDetailsId AND DeliveryPlanDetails.DeliveryPlanId = DeliveryTracker.DeliveryPlanId
    Left Join Office ON Office.OfficeId = DeliveryPlanDetails.officeId
    WHERE
        DeliveryTracker.DeliveryPlanId = {DeliveryPlanId}
    Order By DeliveryTracker.locationUpdateTime''',cnxn)

    if(len(driver_df)>0):
        driver_df[["locationUpdateTime"]]=driver_df[["locationUpdateTime"]].apply(pd.to_datetime)
        driver_df["locationUpdateTime"]=driver_df["locationUpdateTime"].dt.strftime(date_format)
    
    return Delivery_df.to_dict(orient="records"),driver_df.to_dict(orient="records")