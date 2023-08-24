import pandas as pd

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
        df_by_name_mask=df_by_name_mask.groupby('CustomerName',as_index=False).agg(totalCount=('CustomerName','count'))

        ExistingCustomerByName=df_by_name_mask[df_by_name_mask['CustomerName'].isin(Previous_df_byname)]
        NewCustomerByName=df_by_name_mask[~df_by_name_mask['CustomerName'].isin(Previous_df_byname)]

    df_by_mobile_mask=Current_df[Current_df["MobileNo"]!=""]
    if not df_by_mobile_mask.empty:
        df_by_mobile_mask["MobileNo"]=df_by_mobile_mask["MobileNo"].str.replace(' ', '')
        df_by_mobile_mask=df_by_mobile_mask[df_by_mobile_mask["MobileNo"]!="0000"]
        df_by_mobile_mask=df_by_mobile_mask.groupby('MobileNo',as_index=False).agg(totalCount=('MobileNo','count'))

        ExistingCustomerByMobile=df_by_mobile_mask[df_by_mobile_mask['MobileNo'].isin(Previous_df_bymobile)]
        NewCustomerByMobile=df_by_mobile_mask[~df_by_mobile_mask['MobileNo'].isin(Previous_df_bymobile)]
    
    df_by_vehicle_mask=Current_df[Current_df["VehicleNo"]!=""]
    if not df_by_vehicle_mask.empty:
        df_by_vehicle_mask["VehicleNo"]=df_by_vehicle_mask["VehicleNo"].str.replace(' ', '').str.upper()
        df_by_vehicle_mask=df_by_vehicle_mask[df_by_vehicle_mask["VehicleNo"]!="XXX"]
        df_by_vehicle_mask=df_by_vehicle_mask.groupby('VehicleNo',as_index=False).agg(totalCount=('VehicleNo','count'))

        ExistingCustomerByVehicle=df_by_vehicle_mask[df_by_vehicle_mask['VehicleNo'].isin(Previous_df_byvehicle)]
        NewCustomerByVehicle=df_by_vehicle_mask[~df_by_vehicle_mask['VehicleNo'].isin(Previous_df_byvehicle)]

    return {
        'graph1':{
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
        },
        "graph2":[]
    }

def ExistingCurrentCustomer(office_id,is_admin,from_date,to_date,cnxn):

    if is_admin==6:
        Previous_df=PreviousCustomer_Traverse_list(office_id,from_date,-1,cnxn)
        Current_df=CurrentCustomer_Traverse_list(office_id,from_date,to_date,-1,cnxn)
        df=ExistingCurrentCustomer_body(Previous_df,Current_df)


    elif is_admin==4:
        Previous_df=PreviousCustomer_Traverse_list(office_id,from_date,-1,cnxn)
        Current_df=CurrentCustomer_Traverse_list(office_id,from_date,to_date,-1,cnxn)
        Previous_df=Previous_df[~((Previous_df["officeType"]!="Company")& (Previous_df["masterOfficeId"].str.lower()==office_id.lower()))]
        Current_df=Current_df[~((Current_df["officeType"]!="Company")& (Current_df["masterOfficeId"].str.lower()==office_id.lower()))]
        df=ExistingCurrentCustomer_body(Previous_df,Current_df)

    elif is_admin==5:
        Previous_df=PreviousCustomer_Traverse_list(office_id,from_date,-1,cnxn)
        Current_df=CurrentCustomer_Traverse_list(office_id,from_date,to_date,-1,cnxn)
        df=ExistingCurrentCustomer_body(Previous_df,Current_df)

    elif is_admin==1:
        Previous_df=PreviousCustomer_Traverse_list(office_id,from_date,1,cnxn)
        Current_df=CurrentCustomer_Traverse_list(office_id,from_date,to_date,1,cnxn)
        Previous_df=Previous_df[(Previous_df["officeType"]=="Wholesale Pumps")| (Previous_df["officeType"]=="Retail Pumps")]
        Current_df=Current_df[(Current_df["officeType"]=="Wholesale Pumps")| (Current_df["officeType"]=="Retail Pumps")]
        df=ExistingCurrentCustomer_body(Previous_df,Current_df)

    elif is_admin==3:
        Previous_df=PreviousCustomer_Traverse_list(office_id,from_date,1,cnxn)
        Current_df=CurrentCustomer_Traverse_list(office_id,from_date,to_date,1,cnxn)
    
        Previous_df=Previous_df[Previous_df["officeType"]=="Wholesale Pumps"]
        Current_df=Current_df[Current_df["officeType"]=="Wholesale Pumps"]
        df=ExistingCurrentCustomer_body(Previous_df,Current_df)

    elif is_admin==2:
        Previous_df=PreviousCustomer_Traverse_list(office_id,from_date,1,cnxn)
        Current_df=CurrentCustomer_Traverse_list(office_id,from_date,to_date,1,cnxn)
        Previous_df=Previous_df[Previous_df["officeType"]=="Retail Pumps"]
        Current_df=Current_df[Current_df["officeType"]=="Retail Pumps"]
        df=ExistingCurrentCustomer_body(Previous_df,Current_df)

    elif is_admin==0:
        Previous_df=PreviousCustomer_Traverse_list(office_id,from_date,1,cnxn)
        Current_df=CurrentCustomer_Traverse_list(office_id,from_date,to_date,1,cnxn)
        Previous_df=Previous_df[Previous_df["officeId"].str.lower()==office_id.lower()]
        Current_df=Current_df[Current_df["officeId"].str.lower()==office_id.lower()]
        df=ExistingCurrentCustomer_body(Previous_df,Current_df)

    return df
    
    