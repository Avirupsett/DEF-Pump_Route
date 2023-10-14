import pandas as pd
import datetime as dt

def query_deliveries(from_date,to_date,cnxn):
    deliver_df1=pd.read_sql_query(f'''
    SELECT

      dp.PlanTitle,
      dp.DeliveryLimit,
      dp.ContainerSize,
      dp.ProductId,
      dp.StartPointId,
      dp.DeliveryPlanTypeId,
      dp.DeliveryPlanId,
      dp.ExpectedDeliveryDate As StartTime,
      h.HubName,
      h.Latitude AS HubLatitude,
      h.Longitude AS HubLongitude,
      dpd.OfficeId,
      dpd.ApprovedQuantity,
      dpd.DeliveredQuantity,
      dpd.DeliveredAt,
      dpd.ExpectedDeliveryTime,
      o.OfficeName,
      o.OfficeAddress,
      o.Latitude As OfficeLatitude,
      o.Longitude As OfficeLongitude

      FROM DeliveryPlan dp
      LEFT JOIN
      Hub h ON h.HubId=dp.StartPointId
      LEFT JOIN
      DeliveryPlanDetails dpd ON dpd.DeliveryPlanId=dp.DeliveryPlanId
      LEFT JOIN
      Office o ON o.OfficeId=dpd.OfficeId

       Where dp.IsDeleted=0 AND dpd.DeliveredAt>='{from_date}' AND dpd.DeliveredAt<='{to_date}'

    ''',cnxn)

    return deliver_df1

def deliveriesByHub(df):
    grouped_data=[]
    if(len(df)>0):
        df = df[["DeliveryPlanId","StartTime","HubName","OfficeName","DeliveredAt","ExpectedDeliveryTime"]]
        # Group the data by HubName and OfficeName and count the number of deliveries
        grouped_data = df.groupby(['HubName', 'OfficeName']).size().unstack(fill_value=0)
        grouped_data = grouped_data.reset_index().to_dict(orient='records')

    return grouped_data

def deliveriesCount(df,from_date,to_date):
    deliveryCount=[]
    alldata=[]
    if(len(df)>0):
        df = df[["DeliveryPlanId","StartTime","HubName","OfficeName","DeliveredAt","ExpectedDeliveryTime"]]
        df['StartTime']=df['DeliveredAt'].dt.date
        deliveryCount=df.groupby(df['StartTime']).agg(totalDeliveries=('StartTime','count'),date=('StartTime',lambda x:list(x)[0].strftime('%Y-%m-%d')))
        date_range=pd.date_range(from_date,to_date)
        deliveryCount.index=pd.to_datetime(deliveryCount.index)
        for i in date_range:
            alldata.append({
                            "requestedDate": pd.to_datetime(i).strftime("%Y-%m-%d"),
                            "totalDeliveries": int(deliveryCount.loc[pd.to_datetime(i).strftime("%Y-%m-%d")]["totalDeliveries"]) if pd.to_datetime(i).strftime("%Y-%m-%d") in deliveryCount.index else 0,
                        })

    return alldata

def OfficeWiseSales(from_date,to_date,cnxn):
    df=pd.read_sql_query(f'''
    SELECT
    o.officeId,
    o.officeName,
    SUM(S.total) AS total,
    SUM(S.quantity) AS quantity
	
    FROM Office O

    INNER JOIN Sales S ON S.isDeleted=0 AND O.officeId=S.officeId AND O.IsActive=1
    WHERE S.InvoiceDate BETWEEN '{from_date}' AND '{to_date}'
    GROUP BY o.OfficeId, o.officeName

    ORDER BY total DESC''',cnxn)

    return {"top5":df.head(5).to_dict(orient='records'),"bottom5":df.tail(5).to_dict(orient='records'),"top10":df.head(10).to_dict(orient='records'),"bottom10":df.tail(10).to_dict(orient='records')}

def luxusDetails(from_date,to_date,cnxn):
    df=query_deliveries(from_date,to_date,cnxn)
    graph1=deliveriesByHub(df)
    graph2=deliveriesCount(df,from_date,to_date)
    graph3=OfficeWiseSales(from_date,to_date,cnxn)

    return {
        "graph1":graph1,
        "graph2":graph2,
        "graph3":graph3
    }