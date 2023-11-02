import pandas as pd
from datetime import datetime,date
import numpy as np

def PreviousCustomer_Traverse_list(office_id,from_date,level,cnxn):
    # start_time=time.time()
    Sales_df1=pd.read_sql_query(f'''
  WITH cte_org AS (
    SELECT
        ofs.OfficeId,
        ofs.MasterOfficeId,
        ofs.OfficeTypeId,
        ofs.OfficeName,
        mo.OfficeName AS MasterOfficeName,
        0 AS Level,
        ofs.OfficeAddress,
        ofs.RegisteredAddress,
        ofs.OfficeContactNo,
        ofs.OfficeEmail,
        ofs.GSTNumber,
        ofs.IsActive,
        ofs.Latitude,
        ofs.Longitude,
        ofs.GstTypeId
    FROM Office ofs
    LEFT JOIN Office mo ON mo.OfficeId = ofs.MasterOfficeId
    WHERE ofs.OfficeId = '{office_id}'

    UNION ALL

    SELECT
        e.OfficeId,
        e.MasterOfficeId,
        e.OfficeTypeId,
        e.OfficeName,
        o.OfficeName AS MasterOfficeName,
        Level + 1 AS Level,
        e.OfficeAddress,
        e.RegisteredAddress,
        e.OfficeContactNo,
        e.OfficeEmail,
        e.GSTNumber,
        e.IsActive,
        e.Latitude,
        e.Longitude,
        e.GstTypeId
    FROM Office e
    INNER JOIN cte_org o ON o.OfficeId = e.MasterOfficeId
)

SELECT
    ct.IsActive,
    ct.MasterOfficeId As masterOfficeId,
    ct.MasterOfficeName As masterOfficeName,
    ct.OfficeId As officeId,
    ct.OfficeName As officeName,
    ot.OfficeTypeName As officeType,
    ot.color As officeTypeColor,
    ct.level,
	S.InvoiceDate As incomeDate,
	S.Total As totalIncome,
    S.CustomerName,
    S.MobileNo,
    S.VehicleNo
	
FROM cte_org ct
LEFT OUTER JOIN OfficeType ot ON ct.OfficeTypeId = ot.OfficeTypeId

Left Outer join
(
Select Total,Quantity,InvoiceDate,FuelRateId,OfficeId,Rate,
    CustomerName,
    MobileNo,
    VehicleNo,
    PaymentModeId
From Sales
Where
IsDeleted=0 AND
InvoiceDate<'{from_date}'

)S On ct.officeId=S.OfficeId 

WHERE
    ct.IsActive=1 and ({level} < 0 OR ct.Level <= {level})
    ''',cnxn)
    
    # print("Sales Query Time: ",time.time()-start_time)
    return Sales_df1

def CurrentCustomer_Traverse_list(office_id,from_date,to_date,level,cnxn):
    # start_time=time.time()
    Sales_df1=pd.read_sql_query(f'''
  WITH cte_org AS (
    SELECT
        ofs.OfficeId,
        ofs.MasterOfficeId,
        ofs.OfficeTypeId,
        ofs.OfficeName,
        mo.OfficeName AS MasterOfficeName,
        0 AS Level,
        ofs.OfficeAddress,
        ofs.RegisteredAddress,
        ofs.OfficeContactNo,
        ofs.OfficeEmail,
        ofs.GSTNumber,
        ofs.IsActive,
        ofs.Latitude,
        ofs.Longitude,
        ofs.GstTypeId
    FROM Office ofs
    LEFT JOIN Office mo ON mo.OfficeId = ofs.MasterOfficeId
    WHERE ofs.OfficeId = '{office_id}'

    UNION ALL

    SELECT
        e.OfficeId,
        e.MasterOfficeId,
        e.OfficeTypeId,
        e.OfficeName,
        o.OfficeName AS MasterOfficeName,
        Level + 1 AS Level,
        e.OfficeAddress,
        e.RegisteredAddress,
        e.OfficeContactNo,
        e.OfficeEmail,
        e.GSTNumber,
        e.IsActive,
        e.Latitude,
        e.Longitude,
        e.GstTypeId
    FROM Office e
    INNER JOIN cte_org o ON o.OfficeId = e.MasterOfficeId
)

SELECT
    ct.IsActive,
    ct.MasterOfficeId As masterOfficeId,
    ct.MasterOfficeName As masterOfficeName,
    ct.OfficeId As officeId,
    ct.OfficeName As officeName,
    ot.OfficeTypeName As officeType,
    ot.color As officeTypeColor,
    ct.level,
	S.InvoiceDate As incomeDate,
	S.Total As totalIncome,
    S.CustomerName,
    S.MobileNo,
    S.VehicleNo,
    S.InvoiceNo
	
FROM cte_org ct
LEFT OUTER JOIN OfficeType ot ON ct.OfficeTypeId = ot.OfficeTypeId

Left Outer join
(
Select Total,Quantity,InvoiceDate,FuelRateId,OfficeId,Rate,
    CustomerName,
    MobileNo,
    VehicleNo,
    PaymentModeId,
    InvoiceNo
From Sales
Where
IsDeleted=0 AND
InvoiceDate>='{from_date}' AND InvoiceDate<='{to_date}'

)S On ct.officeId=S.OfficeId 

WHERE
    ct.IsActive=1 and ({level} < 0 OR ct.Level <= {level})
    ''',cnxn)
    
    # print("Sales Query Time: ",time.time()-start_time)
    return Sales_df1

def ExistingCurrentCustomer_body(Previous_df,Current_df):
    ExistingCustomerByName=pd.DataFrame()
    NewCustomerByName=pd.DataFrame()
    ExistingCustomerByMobile=pd.DataFrame()
    NewCustomerByMobile=pd.DataFrame()
    ExistingCustomerByVehicle=pd.DataFrame()
    NewCustomerByVehicle=pd.DataFrame()
    if not Previous_df.empty:
        Previous_df['CustomerName']=Previous_df['CustomerName'].str.upper().str.strip()
        Previous_df_byname=Previous_df[(Previous_df["CustomerName"]!="XXX")|(Previous_df["CustomerName"]!="")]
        Previous_df_byname=Previous_df_byname['CustomerName'].unique()

        Previous_df["MobileNo"]=Previous_df["MobileNo"].str.replace(' ', '')
        Previous_df_bymobile=Previous_df[(Previous_df["MobileNo"]!="0000")|(Previous_df["MobileNo"]!="")]
        Previous_df_bymobile=Previous_df_bymobile["MobileNo"].unique()

        Previous_df["VehicleNo"]=Previous_df["VehicleNo"].str.replace(' ', '').str.upper()
        Previous_df_byvehicle=Previous_df[(Previous_df["VehicleNo"]!="XXX")|(Previous_df["VehicleNo"]!="")]
        Previous_df_byvehicle=Previous_df_byvehicle["VehicleNo"].unique()

    df_by_name_mask=Current_df[Current_df["CustomerName"]!=""]
    if not df_by_name_mask.empty:
        df_by_name_mask["CustomerName"]=df_by_name_mask["CustomerName"].str.upper().str.strip()
        df_by_name_mask=df_by_name_mask[df_by_name_mask["CustomerName"]!="XXX"]
        df_by_name_mask=df_by_name_mask.groupby('CustomerName',as_index=False).agg(totalCount=('CustomerName','count'),totalSales=('totalIncome','sum'))

        ExistingCustomerByName=df_by_name_mask[df_by_name_mask['CustomerName'].isin(Previous_df_byname)]
        NewCustomerByName=df_by_name_mask[~df_by_name_mask['CustomerName'].isin(Previous_df_byname)]

    df_by_mobile_mask=Current_df[Current_df["MobileNo"]!=""]
    if not df_by_mobile_mask.empty:
        df_by_mobile_mask["MobileNo"]=df_by_mobile_mask["MobileNo"].str.replace(' ', '')
        df_by_mobile_mask=df_by_mobile_mask[df_by_mobile_mask["MobileNo"]!="0000"]
        df_by_mobile_mask=df_by_mobile_mask.groupby('MobileNo',as_index=False).agg(totalCount=('MobileNo','count'),totalSales=('totalIncome','sum'))

        ExistingCustomerByMobile=df_by_mobile_mask[df_by_mobile_mask['MobileNo'].isin(Previous_df_bymobile)]
        NewCustomerByMobile=df_by_mobile_mask[~df_by_mobile_mask['MobileNo'].isin(Previous_df_bymobile)]
    
    df_by_vehicle_mask=Current_df[Current_df["VehicleNo"]!=""]
    if not df_by_vehicle_mask.empty:
        df_by_vehicle_mask["VehicleNo"]=df_by_vehicle_mask["VehicleNo"].str.replace(' ', '').str.upper()
        df_by_vehicle_mask=df_by_vehicle_mask[df_by_vehicle_mask["VehicleNo"]!="XXX"]
        df_by_vehicle_mask=df_by_vehicle_mask.groupby('VehicleNo',as_index=False).agg(totalCount=('VehicleNo','count'),totalSales=('totalIncome','sum'))

        ExistingCustomerByVehicle=df_by_vehicle_mask[df_by_vehicle_mask['VehicleNo'].isin(Previous_df_byvehicle)]
        NewCustomerByVehicle=df_by_vehicle_mask[~df_by_vehicle_mask['VehicleNo'].isin(Previous_df_byvehicle)]

    return {
            'byName':{
                'existingCustomer':ExistingCustomerByName.to_dict(orient='records'),
                'newCustomer':NewCustomerByName.to_dict(orient='records'),
                'existingCustomerCount':int(ExistingCustomerByName["totalCount"].sum()) if len(ExistingCustomerByName)>0 else 0,
                'newCustomerCount':int(NewCustomerByName["totalCount"].sum()) if len(NewCustomerByName)>0 else 0
            },
            'byMobile':{
                'existingCustomer':ExistingCustomerByMobile.to_dict(orient='records'),
                'newCustomer':NewCustomerByMobile.to_dict(orient='records'),
                'existingCustomerCount':int(ExistingCustomerByMobile["totalCount"].sum()) if len(ExistingCustomerByMobile)>0 else 0,
                'newCustomerCount':int(NewCustomerByMobile["totalCount"].sum()) if len(NewCustomerByMobile)>0 else 0
            },
            'byVehicle':{
                'existingCustomer':ExistingCustomerByVehicle.to_dict(orient='records'),
                'newCustomer':NewCustomerByVehicle.to_dict(orient='records'),
                'existingCustomerCount':int(ExistingCustomerByVehicle["totalCount"].sum()) if len(ExistingCustomerByVehicle)>0 else 0,
                'newCustomerCount':int(NewCustomerByVehicle["totalCount"].sum()) if len(NewCustomerByVehicle)>0 else 0
            }

    }

def ExistingCurrentCustomer_Daywise_body(Previous_df,Current_df,date_range):
    ExistingCustomerByName=pd.DataFrame()
    NewCustomerByName=pd.DataFrame()
    ExistingCustomerByMobile=pd.DataFrame()
    NewCustomerByMobile=pd.DataFrame()
    ExistingCustomerByVehicle=pd.DataFrame()
    NewCustomerByVehicle=pd.DataFrame()

    if not Previous_df.empty:
        Previous_df['CustomerName']=Previous_df['CustomerName'].str.upper()
        Previous_df_byname=Previous_df[(Previous_df["CustomerName"]!="XXX")|(Previous_df["CustomerName"]!="")]
        Previous_df_byname=Previous_df_byname['CustomerName'].unique()

        Previous_df["MobileNo"]=Previous_df["MobileNo"].str.replace(' ', '')
        Previous_df_bymobile=Previous_df[(Previous_df["MobileNo"]!="0000")|(Previous_df["MobileNo"]!="")]
        Previous_df_bymobile=Previous_df_bymobile["MobileNo"].unique()

        Previous_df["VehicleNo"]=Previous_df["VehicleNo"].str.replace(' ', '').str.upper()
        Previous_df_byvehicle=Previous_df[(Previous_df["VehicleNo"]!="XXX")|(Previous_df["VehicleNo"]!="")]
        Previous_df_byvehicle=Previous_df_byvehicle["VehicleNo"].unique()

    df_by_name_mask=Current_df[Current_df["CustomerName"]!=""]
    if not df_by_name_mask.empty:
        df_by_name_mask["CustomerName"]=df_by_name_mask["CustomerName"].str.upper()
        df_by_name_mask=df_by_name_mask[df_by_name_mask["CustomerName"]!="XXX"]
        df_by_name_mask=df_by_name_mask.groupby(['incomeDate','CustomerName'],as_index=False).agg(totalCount=('CustomerName','count'),totalSales=('totalIncome','sum'))

        ExistingCustomerByName=df_by_name_mask[df_by_name_mask['CustomerName'].isin(Previous_df_byname)]
        NewCustomerByName=df_by_name_mask[~df_by_name_mask['CustomerName'].isin(Previous_df_byname)]
        
        ExistingCustomerByName=ExistingCustomerByName.groupby('incomeDate').agg(count=('incomeDate','count'),totalSales=('totalSales','sum'))
        NewCustomerByName=NewCustomerByName.groupby('incomeDate').agg(count=('incomeDate','count'),totalSales=('totalSales','sum'))

    df_by_mobile_mask=Current_df[Current_df["MobileNo"]!=""]
    if not df_by_mobile_mask.empty:
        df_by_mobile_mask["MobileNo"]=df_by_mobile_mask["MobileNo"].str.replace(' ', '')
        df_by_mobile_mask=df_by_mobile_mask[df_by_mobile_mask["MobileNo"]!="0000"]
        df_by_mobile_mask=df_by_mobile_mask.groupby(['incomeDate','MobileNo'],as_index=False).agg(totalCount=('MobileNo','count'),totalSales=('totalIncome','sum'))

        ExistingCustomerByMobile=df_by_mobile_mask[df_by_mobile_mask['MobileNo'].isin(Previous_df_bymobile)]
        NewCustomerByMobile=df_by_mobile_mask[~df_by_mobile_mask['MobileNo'].isin(Previous_df_bymobile)]
        
        ExistingCustomerByMobile=ExistingCustomerByMobile.groupby('incomeDate').agg(count=('incomeDate','count'),totalSales=('totalSales','sum'))
        NewCustomerByMobile=NewCustomerByMobile.groupby('incomeDate').agg(count=('incomeDate','count'),totalSales=('totalSales','sum'))
    
    df_by_vehicle_mask=Current_df[Current_df["VehicleNo"]!=""]
    if not df_by_vehicle_mask.empty:
        df_by_vehicle_mask["VehicleNo"]=df_by_vehicle_mask["VehicleNo"].str.replace(' ', '').str.upper()
        df_by_vehicle_mask=df_by_vehicle_mask[df_by_vehicle_mask["VehicleNo"]!="XXX"]
        df_by_vehicle_mask=df_by_vehicle_mask.groupby(['incomeDate','VehicleNo'],as_index=False).agg(totalCount=('VehicleNo','count'),totalSales=('totalIncome','sum'))

        ExistingCustomerByVehicle=df_by_vehicle_mask[df_by_vehicle_mask['VehicleNo'].isin(Previous_df_byvehicle)]
        NewCustomerByVehicle=df_by_vehicle_mask[~df_by_vehicle_mask['VehicleNo'].isin(Previous_df_byvehicle)]
        
        ExistingCustomerByVehicle=ExistingCustomerByVehicle.groupby('incomeDate').agg(count=('incomeDate','count'),totalSales=('totalSales','sum'))
        NewCustomerByVehicle=NewCustomerByVehicle.groupby('incomeDate').agg(count=('incomeDate','count'),totalSales=('totalSales','sum'))

   
    return {
            'byName':{
                'existingCustomer':[{"requestedDate":datetime.strftime(i,"%Y-%m-%d"),"count":int(ExistingCustomerByName.loc[i,"count"]),"sales":ExistingCustomerByName.loc[i,"totalSales"]} if i in ExistingCustomerByName.index else {"requestedDate":datetime.strftime(i,"%Y-%m-%d"),"count":0,"sales":0}  for i in date_range],
                'newCustomer':[{"requestedDate":datetime.strftime(i,"%Y-%m-%d"),"count":int(NewCustomerByName.loc[i,"count"]),"sales":NewCustomerByName.loc[i,"totalSales"]} if i in NewCustomerByName.index else {"requestedDate":datetime.strftime(i,"%Y-%m-%d"),"count":0,"sales":0}  for i in date_range]
            },
            'byMobile':{
                'existingCustomer':[{"requestedDate":datetime.strftime(i,"%Y-%m-%d"),"count":int(ExistingCustomerByMobile.loc[i,"count"]),"sales":ExistingCustomerByMobile.loc[i,"totalSales"]} if i in ExistingCustomerByMobile.index else {"requestedDate":datetime.strftime(i,"%Y-%m-%d"),"count":0,"sales":0}  for i in date_range],
                'newCustomer':[{"requestedDate":datetime.strftime(i,"%Y-%m-%d"),"count":int(NewCustomerByMobile.loc[i,"count"]),"sales":NewCustomerByMobile.loc[i,"totalSales"]} if i in NewCustomerByMobile.index else {"requestedDate":datetime.strftime(i,"%Y-%m-%d"),"count":0,"sales":0}  for i in date_range]
            },
            'byVehicle':{
                'existingCustomer':[{"requestedDate":datetime.strftime(i,"%Y-%m-%d"),"count":int(ExistingCustomerByVehicle.loc[i,"count"]),"sales":ExistingCustomerByVehicle.loc[i,"totalSales"]} if i in ExistingCustomerByVehicle.index else {"requestedDate":datetime.strftime(i,"%Y-%m-%d"),"count":0,"sales":0}  for i in date_range],
                'newCustomer':[{"requestedDate":datetime.strftime(i,"%Y-%m-%d"),"count":int(NewCustomerByVehicle.loc[i,"count"]),"sales":NewCustomerByVehicle.loc[i,"totalSales"]} if i in NewCustomerByVehicle.index else {"requestedDate":datetime.strftime(i,"%Y-%m-%d"),"count":0,"sales":0}  for i in date_range]
            }

    }

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
    
def CustomerAnalytics(Sales_df1):
    segmented_rfm=pd.DataFrame()
    try:
        NOW=datetime(date.today().year,date.today().month,date.today().day)
        Sales_df1['incomeDate'] = pd.to_datetime(Sales_df1['incomeDate'])
        Sales_df1['MobileNo']=Sales_df1['MobileNo'].str.replace(' ','')
        Sales_df1['CustomerName']=Sales_df1['CustomerName'].str.upper().str.strip()
        Sales_df1['VehicleNo']=Sales_df1['VehicleNo'].str.upper().str.replace(' ','')

        Sales_df1['MobileNo']=np.where(~((Sales_df1['MobileNo']=="")|(Sales_df1['MobileNo']=="0000")),Sales_df1['MobileNo'],"_UnKnown"+Sales_df1['VehicleNo']+Sales_df1['CustomerName'])
        Sales_df1['MobileNo']=np.where(((Sales_df1['MobileNo'].str.startswith('_UnKnown'))&(Sales_df1['VehicleNo']!="")&(Sales_df1['VehicleNo']!="XXX")),"_UnKnown"+Sales_df1['VehicleNo'],Sales_df1['MobileNo'])
        
        Sales_df1=Sales_df1[((Sales_df1["MobileNo"]!="")&(Sales_df1["VehicleNo"]!="")&(Sales_df1["VehicleNo"]!="XXX")&(Sales_df1["CustomerName"]!="")&(Sales_df1["CustomerName"]!="XXX"))]
        # Sales_df1=Custom_MobileNo_df[~((Custom_MobileNo_df['MobileNo']=="")|(Custom_MobileNo_df['MobileNo']=="_UnKnown")|(Custom_MobileNo_df['MobileNo']=="0000"))]
        # Sales_df1=Sales_df1[~((Sales_df1['MobileNo']=="")|(Sales_df1['MobileNo']=="0000"))]

        rfmTable = Sales_df1.groupby('MobileNo',as_index=False).agg({'incomeDate': lambda x: (NOW - x.max()).days, 'InvoiceNo': lambda x: len(x), 'totalIncome': lambda x: x.sum(),'VehicleNo':lambda x: list(filter(None,list(set(x)))),'CustomerName':lambda x: list(filter(None,list(set(x))))})
        rfmTable['incomeDate'] = rfmTable['incomeDate'].astype(int)
        rfmTable.rename(columns={'incomeDate': 'Last Visit',
                                'InvoiceNo': 'Total Visits',
                                'totalIncome': 'Sales',
                                'CustomerName':'Name'}, inplace=True)
        
        quantiles = rfmTable.quantile(q=[0.25,0.5,0.75])
        quantiles = quantiles.to_dict()
        segmented_rfm = rfmTable
        segmented_rfm['r_quartile'] = segmented_rfm['Last Visit'].apply(RScore, args=('Last Visit',quantiles,))
        segmented_rfm['f_quartile'] = segmented_rfm['Total Visits'].apply(FMScore, args=('Total Visits',quantiles,))
        segmented_rfm['m_quartile'] = segmented_rfm['Sales'].apply(FMScore, args=('Sales',quantiles,))

        segmented_rfm['RFMScore'] = segmented_rfm.r_quartile.map(str) + segmented_rfm.f_quartile.map(str) + segmented_rfm.m_quartile.map(str)
        segmented_rfm['MobileNo'].mask(segmented_rfm['MobileNo'].str.startswith('_UnKnown'),'',inplace=True)
    except:
        print("No Data for Customer Analytics")
    return {'Most Valuable Customers':segmented_rfm[segmented_rfm['RFMScore']=='111'].sort_values('Sales', ascending=False)[["Name","MobileNo","VehicleNo","Last Visit","Total Visits","Sales"]].to_dict(orient='records') if len(segmented_rfm)>0 else [],
           'Most Frequent Customers':segmented_rfm[segmented_rfm['f_quartile']==1].sort_values('Sales', ascending=False)[["Name","MobileNo","VehicleNo","Last Visit","Total Visits","Sales"]].to_dict(orient='records') if len(segmented_rfm)>0 else [],
           'Big Buyers':segmented_rfm[segmented_rfm['m_quartile']==1].sort_values('Sales', ascending=False)[["Name","MobileNo","VehicleNo","Last Visit","Total Visits","Sales"]].to_dict(orient='records') if len(segmented_rfm)>0 else [],
           'Non Follow-Up Customers':segmented_rfm[segmented_rfm['RFMScore']=='311'].sort_values('Sales', ascending=False)[["Name","MobileNo","VehicleNo","Last Visit","Total Visits","Sales"]].to_dict(orient='records') if len(segmented_rfm)>0 else [],
           'Former Customers':segmented_rfm[segmented_rfm['RFMScore']=='411'].sort_values('Sales', ascending=False)[["Name","MobileNo","VehicleNo","Last Visit","Total Visits","Sales"]].to_dict(orient='records') if len(segmented_rfm)>0 else [],
          'Former Low-Value Customers':segmented_rfm[segmented_rfm['RFMScore']=='444'].sort_values('Sales', ascending=False)[["Name","MobileNo","VehicleNo","Last Visit","Total Visits","Sales"]].to_dict(orient='records') if len(segmented_rfm)>0 else []
    }

def get_month(x):
    return datetime(int(x.year), int(x.month), 1)

def get_date_int(df, column):
    year = df[column].dt.year
    month = df[column].dt.month
    date = df[column].dt.date
    return year, month, date
def MonthlyRecurringCustomer(Sales_df1):
    Sales_df1['CustomerName']=Sales_df1['CustomerName'].str.upper().str.strip()
    Sales_df1['VehicleNo']=Sales_df1['VehicleNo'].str.upper().str.strip()
    Sales_df1["MobileNo"]=Sales_df1["MobileNo"].str.replace(' ', '')

    Sales_df1['MobileNo']=np.where(~((Sales_df1['MobileNo']=="")|(Sales_df1['MobileNo']=="0000")),Sales_df1['MobileNo'],"_UnKnown"+Sales_df1['VehicleNo']+Sales_df1['CustomerName'])
    Sales_df1['MobileNo']=np.where(((Sales_df1['MobileNo'].str.startswith('_UnKnown'))&(Sales_df1['VehicleNo']!="")&(Sales_df1['VehicleNo']!="XXX")),"_UnKnown"+Sales_df1['VehicleNo'],Sales_df1['MobileNo'])

    Sales_df1.dropna(inplace=True,subset=['incomeDate'],axis=0)
    if len(Sales_df1)==0:
        return []
    Sales_df1['InvoiceMonth'] = Sales_df1['incomeDate'].apply(get_month)

    grouping = Sales_df1.groupby('MobileNo')['InvoiceMonth']
    Sales_df1['CohortMonth'] = grouping.transform('min')

    Invoice_Year, Invoice_Month, _ = get_date_int(Sales_df1, 'InvoiceMonth')
    Cohort_Year, Cohort_Month , _ = get_date_int(Sales_df1, 'CohortMonth')
    Year_Diff = Invoice_Year - Cohort_Year
    Month_Diff = Invoice_Month - Cohort_Month
    Sales_df1['CohortIndex'] = Year_Diff*12 + Month_Diff +1

    grouping = Sales_df1.groupby(['CohortMonth', 'CohortIndex'])
    cohort_data = grouping['MobileNo'].apply(pd.Series.nunique)
    cohort_data = cohort_data.reset_index()

    cohort_counts = cohort_data.pivot(index="CohortMonth",
                                  columns="CohortIndex",
                                  values="MobileNo")
    retention = cohort_counts
    cohort_sizes = cohort_counts.iloc[:,0]
    retention = cohort_counts.divide(cohort_sizes, axis=0)
    retention=retention.round(3)*100

    datetime_index = retention.index
    formatted_dates = [date.strftime('%b %Y') for date in datetime_index]

    df=retention.reset_index(drop=True)
    df.fillna(0,inplace=True)

    # Initialize an empty list to store the result
    result = []

    # Loop through the DataFrame to create the desired format
    for row_index, row in df[::-1].iterrows():
        for col_index, value in enumerate(row):
            rounded_value = round(value)
            result.append([df.shape[0]-row_index-1, col_index, rounded_value])

    return {'data':result,'formatted_dates':formatted_dates[::-1],'cohort_index':[i for i in range(1,df.shape[1]+1)]}

def get_day(x):
    return x.date

def RecurringCustomerDaily(Sales_df1):
    Sales_df1['CustomerName'] = Sales_df1['CustomerName'].str.upper().str.strip()
    Sales_df1['VehicleNo'] = Sales_df1['VehicleNo'].str.upper().str.strip()
    Sales_df1["MobileNo"] = Sales_df1["MobileNo"].str.replace(' ', '')

    Sales_df1['MobileNo'] = np.where(~((Sales_df1['MobileNo'] == "") | (Sales_df1['MobileNo'] == "0000")), Sales_df1['MobileNo'], "_UnKnown" + Sales_df1['VehicleNo'] + Sales_df1['CustomerName'])
    Sales_df1['MobileNo'] = np.where(((Sales_df1['MobileNo'].str.startswith('_UnKnown')) & (Sales_df1['VehicleNo'] != "") & (Sales_df1['VehicleNo'] != "XXX")), "_UnKnown" + Sales_df1['VehicleNo'], Sales_df1['MobileNo'])
    
    Sales_df1.dropna(inplace=True,subset=['incomeDate'],axis=0)
    if len(Sales_df1)==0:
        return []
    Sales_df1['InvoiceDay'] = Sales_df1['incomeDate']

    grouping = Sales_df1.groupby('MobileNo')['InvoiceDay']
    Sales_df1['CohortDay'] = grouping.transform('min')

    Invoice_Year, Invoice_Month, Invoice_Day = get_date_int(Sales_df1, 'InvoiceDay')
    Cohort_Year, Cohort_Month, Cohort_Day = get_date_int(Sales_df1, 'CohortDay')
    Year_Diff = Invoice_Year - Cohort_Year
    Month_Diff = Invoice_Month - Cohort_Month
    Day_Diff = Invoice_Day - Cohort_Day

    Sales_df1['CohortIndex'] = Year_Diff * 365 + Month_Diff * 30 + Day_Diff.dt.days + 1

    grouping = Sales_df1.groupby(['CohortDay', 'CohortIndex'])
    cohort_data = grouping['MobileNo'].apply(pd.Series.nunique)
    cohort_data = cohort_data.reset_index()

    cohort_counts = cohort_data.pivot(index="CohortDay",
                                      columns="CohortIndex",
                                      values="MobileNo")
    retention = cohort_counts
    cohort_sizes = cohort_counts.iloc[:, 0]
    retention = cohort_counts.divide(cohort_sizes, axis=0)
    retention = retention.round(3) * 100

    datetime_index = retention.index
    formatted_dates = [date.strftime('%b %d, %Y') for date in datetime_index]

    df = retention.reset_index(drop=True)
    df.fillna(0, inplace=True)

    # Initialize an empty list to store the result
    result = []

    # Loop through the DataFrame to create the desired format
    for row_index, row in df[::-1].iterrows():
        for col_index, value in enumerate(row):
            rounded_value = round(value)
            result.append([df.shape[0] - row_index - 1, col_index, rounded_value])

    return {'data': result, 'formatted_dates': formatted_dates[::-1], 'cohort_index': [i for i in range(1, df.shape[1] + 1)]}

def get_year(x):
    return datetime(x.year, 1, 1)

def RecurringCustomerYearly(Sales_df1):
    Sales_df1['CustomerName'] = Sales_df1['CustomerName'].str.upper().str.strip()
    Sales_df1['VehicleNo'] = Sales_df1['VehicleNo'].str.upper().str.strip()
    Sales_df1["MobileNo"] = Sales_df1["MobileNo"].str.replace(' ', '')

    Sales_df1['MobileNo'] = np.where(~((Sales_df1['MobileNo'] == "") | (Sales_df1['MobileNo'] == "0000")), Sales_df1['MobileNo'], "_UnKnown" + Sales_df1['VehicleNo'] + Sales_df1['CustomerName'])
    Sales_df1['MobileNo'] = np.where((Sales_df1['MobileNo'].str.startswith('_UnKnown')) & (Sales_df1['VehicleNo'] != "") & (Sales_df1['VehicleNo'] != "XXX"), "_UnKnown" + Sales_df1['VehicleNo'], Sales_df1['MobileNo'])
    
    Sales_df1.dropna(inplace=True,subset=['incomeDate'],axis=0)
    if len(Sales_df1)==0:
        return []
    Sales_df1['InvoiceYear'] = Sales_df1['incomeDate'].apply(get_year)

    grouping = Sales_df1.groupby('MobileNo')['InvoiceYear']
    Sales_df1['CohortYear'] = grouping.transform('min')

    Invoice_Year, _, _ = get_date_int(Sales_df1, 'InvoiceYear')
    Cohort_Year, _, _ = get_date_int(Sales_df1, 'CohortYear')
    Year_Diff = Invoice_Year - Cohort_Year
    Sales_df1['CohortIndex'] = Year_Diff + 1

    grouping = Sales_df1.groupby(['CohortYear', 'CohortIndex'])
    cohort_data = grouping['MobileNo'].apply(pd.Series.nunique)
    cohort_data = cohort_data.reset_index()

    cohort_counts = cohort_data.pivot(index="CohortYear",
                                      columns="CohortIndex",
                                      values="MobileNo")
    retention = cohort_counts
    cohort_sizes = cohort_counts.iloc[:, 0]
    retention = cohort_counts.divide(cohort_sizes, axis=0)
    retention = retention.round(3) * 100

    datetime_index = retention.index
    formatted_dates = [date.strftime('%Y') for date in datetime_index]

    df = retention.reset_index(drop=True)
    df.fillna(0, inplace=True)

    # Initialize an empty list to store the result
    result = []

    # Loop through the DataFrame to create the desired format
    for row_index, row in df[::-1].iterrows():
        for col_index, value in enumerate(row):
            rounded_value = round(value)
            result.append([df.shape[0] - row_index - 1, col_index, rounded_value])

    return {'data': result, 'formatted_dates': formatted_dates[::-1], 'cohort_index': [i for i in range(1, df.shape[1] + 1)]}

def ExistingCurrentCustomer(office_id,is_admin,from_date,to_date,cnxn):
    date_range=pd.date_range(from_date,to_date)
    graph4=[]
    graph5=[]
    graph6=[]

    if is_admin==6:
        Previous_df=PreviousCustomer_Traverse_list(office_id,from_date,-1,cnxn)
        Current_df=CurrentCustomer_Traverse_list(office_id,from_date,to_date,-1,cnxn)
        graph1=ExistingCurrentCustomer_body(Previous_df,Current_df)
        graph2=ExistingCurrentCustomer_Daywise_body(Previous_df,Current_df,date_range)
        graph3=CustomerAnalytics(Current_df)
        if(len(Current_df)>0):
            graph4=MonthlyRecurringCustomer(Current_df)
            if (pd.to_datetime(from_date).year != pd.to_datetime(to_date).year):
                graph6=RecurringCustomerYearly(Current_df)
            if(len(date_range)<=31):
                graph5=RecurringCustomerDaily(Current_df)


    elif is_admin==4:
        Previous_df=PreviousCustomer_Traverse_list(office_id,from_date,-1,cnxn)
        Current_df=CurrentCustomer_Traverse_list(office_id,from_date,to_date,-1,cnxn)
        Previous_df=Previous_df[~((Previous_df["officeType"]!="Company")& (Previous_df["masterOfficeId"].str.lower()==office_id.lower()))]
        Current_df=Current_df[~((Current_df["officeType"]!="Company")& (Current_df["masterOfficeId"].str.lower()==office_id.lower()))]
        graph1=ExistingCurrentCustomer_body(Previous_df,Current_df)
        graph2=ExistingCurrentCustomer_Daywise_body(Previous_df,Current_df,date_range)
        graph3=CustomerAnalytics(Current_df)
        if(len(Current_df)>0):
            graph4=MonthlyRecurringCustomer(Current_df)
            if (pd.to_datetime(from_date).year != pd.to_datetime(to_date).year):
                graph6=RecurringCustomerYearly(Current_df)
            if(len(date_range)<=31):
                graph5=RecurringCustomerDaily(Current_df)


    elif is_admin==5:
        Previous_df=PreviousCustomer_Traverse_list(office_id,from_date,-1,cnxn)
        Current_df=CurrentCustomer_Traverse_list(office_id,from_date,to_date,-1,cnxn)
        graph1=ExistingCurrentCustomer_body(Previous_df,Current_df)
        graph2=ExistingCurrentCustomer_Daywise_body(Previous_df,Current_df,date_range)
        graph3=CustomerAnalytics(Current_df)
        if(len(Current_df)>0):
            graph4=MonthlyRecurringCustomer(Current_df)
            if (pd.to_datetime(from_date).year != pd.to_datetime(to_date).year):
                graph6=RecurringCustomerYearly(Current_df)
            if(len(date_range)<=31):
                graph5=RecurringCustomerDaily(Current_df)


    elif is_admin==1:
        Previous_df=PreviousCustomer_Traverse_list(office_id,from_date,1,cnxn)
        Current_df=CurrentCustomer_Traverse_list(office_id,from_date,to_date,1,cnxn)
        Previous_df=Previous_df[(Previous_df["officeType"]=="Wholesale Pumps")| (Previous_df["officeType"]=="Retail Pumps")]
        Current_df=Current_df[(Current_df["officeType"]=="Wholesale Pumps")| (Current_df["officeType"]=="Retail Pumps")]
        graph1=ExistingCurrentCustomer_body(Previous_df,Current_df)
        graph2=ExistingCurrentCustomer_Daywise_body(Previous_df,Current_df,date_range)
        graph3=CustomerAnalytics(Current_df)
        if(len(Current_df)>0):
            graph4=MonthlyRecurringCustomer(Current_df)
            if (pd.to_datetime(from_date).year != pd.to_datetime(to_date).year):
                graph6=RecurringCustomerYearly(Current_df)
            if(len(date_range)<=31):
                graph5=RecurringCustomerDaily(Current_df)


    elif is_admin==3:
        Previous_df=PreviousCustomer_Traverse_list(office_id,from_date,1,cnxn)
        Current_df=CurrentCustomer_Traverse_list(office_id,from_date,to_date,1,cnxn)
    
        Previous_df=Previous_df[Previous_df["officeType"]=="Wholesale Pumps"]
        Current_df=Current_df[Current_df["officeType"]=="Wholesale Pumps"]
        graph1=ExistingCurrentCustomer_body(Previous_df,Current_df)
        graph2=ExistingCurrentCustomer_Daywise_body(Previous_df,Current_df,date_range)
        graph3=CustomerAnalytics(Current_df)
        if(len(Current_df)>0):
            graph4=MonthlyRecurringCustomer(Current_df)
            if (pd.to_datetime(from_date).year != pd.to_datetime(to_date).year):
                graph6=RecurringCustomerYearly(Current_df)
            if(len(date_range)<=31):
                graph5=RecurringCustomerDaily(Current_df)


    elif is_admin==2:
        Previous_df=PreviousCustomer_Traverse_list(office_id,from_date,1,cnxn)
        Current_df=CurrentCustomer_Traverse_list(office_id,from_date,to_date,1,cnxn)
        Previous_df=Previous_df[Previous_df["officeType"]=="Retail Pumps"]
        Current_df=Current_df[Current_df["officeType"]=="Retail Pumps"]
        graph1=ExistingCurrentCustomer_body(Previous_df,Current_df)
        graph2=ExistingCurrentCustomer_Daywise_body(Previous_df,Current_df,date_range)
        graph3=CustomerAnalytics(Current_df)
        if(len(Current_df)>0):
            graph4=MonthlyRecurringCustomer(Current_df)
            if (pd.to_datetime(from_date).year != pd.to_datetime(to_date).year):
                graph6=RecurringCustomerYearly(Current_df)
            if(len(date_range)<=31):
                graph5=RecurringCustomerDaily(Current_df)


    elif is_admin==0:
        Previous_df=PreviousCustomer_Traverse_list(office_id,from_date,1,cnxn)
        Current_df=CurrentCustomer_Traverse_list(office_id,from_date,to_date,1,cnxn)
        Previous_df=Previous_df[Previous_df["officeId"].str.lower()==office_id.lower()]
        Current_df=Current_df[Current_df["officeId"].str.lower()==office_id.lower()]
        graph1=ExistingCurrentCustomer_body(Previous_df,Current_df)
        graph2=ExistingCurrentCustomer_Daywise_body(Previous_df,Current_df,date_range)
        graph3=CustomerAnalytics(Current_df)
        if(len(Current_df)>0):
            graph4=MonthlyRecurringCustomer(Current_df)
            if (pd.to_datetime(from_date).year != pd.to_datetime(to_date).year):
                graph6=RecurringCustomerYearly(Current_df)
            if(len(date_range)<=31):
                graph5=RecurringCustomerDaily(Current_df)

    return {"graph1":graph1,"graph2":graph2,"graph3":graph3,"graph4":{"MonthlyRecurringCustomer":graph4,"DaywiseRecurringCustomer":graph5,"YearlyRecurringCustomer":graph6}}
    
    