import pandas as pd
from datetime import datetime, timedelta

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
 

    driver_assigned_df=pd.read_sql_query(f'''
                                Select dp.driverId,dp.deliveryPlanId,dp.expectedDeliveryDate,dp.planDate,
                                d.driverName,d.licenceNo,d.contactNumber 
                                from DeliveryPlan dp
                                Left Join Driver d ON d.DriverId=dp.DriverId
                                Where dp.IsDeleted=0 AND d.IsActive=1 AND (dp.expectedDeliveryDate >='{datetime.strftime(delivery_df['planDate'].iloc[0],date_format)}' AND dp.expectedDeliveryDate <='{datetime.strftime(delivery_df['expectedDeliveryDate'].iloc[0]+timedelta(seconds=1),date_format)}')
                                ORDER BY dp.expectedDeliveryDate Desc;''',cnxn)
    
    
    driver_not_assigned_df=driver_df[~driver_df['driverId'].isin(driver_assigned_df['driverId'])]
    
    return driver_assigned_df.to_dict('records'),driver_not_assigned_df.to_dict('records')
    
