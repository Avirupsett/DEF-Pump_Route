import pandas as pd

def godown_list(office_id,from_date,to_date,level,cnxn):
    df=pd.read_sql_query(f'''
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
    ct.MasterOfficeId As masterOfficeId,
    ct.MasterOfficeName As masterOfficeName,
    ct.OfficeId As officeId,
    ct.OfficeName As officeName,
    ot.OfficeTypeName As officeType,
	S.Total,
    S.CustomerName,
    S.MobileNo,
    S.VehicleNo
	
FROM cte_org ct
LEFT OUTER JOIN OfficeType ot ON ct.OfficeTypeId = ot.OfficeTypeId

Left Outer join (
    Select Total,
    CustomerName,
    MobileNo,
    VehicleNo,
    OfficeId
    From
    Sales Where
IsDeleted=0 AND
InvoiceDate>='{from_date}' AND InvoiceDate<='{to_date}') S
On ct.officeId=S.OfficeId 

WHERE
    ({level} < 0 OR ct.Level <= {level})
    ''',cnxn)
    return df

def total_sales_based_on_customer_body(df):
    df_by_name=pd.DataFrame()
    df_by_mobile=pd.DataFrame()
    df_by_vehicle=pd.DataFrame()
    
    df_by_name_mask=df[df["CustomerName"]!=""]
    if not df_by_name_mask.empty:
        df_by_name_mask["CustomerName"]=df_by_name_mask["CustomerName"].str.upper()
        df_by_name_mask=df_by_name_mask[df_by_name_mask["CustomerName"]!="XXX"]
        df_by_name=df_by_name_mask.groupby(["CustomerName"],as_index=True).agg(total=("Total","sum"),count=("Total","count"),filterName=("CustomerName","first")).sort_values(by=["total"],ascending=True).reset_index(drop=True)[-10:]

    df_by_mobile_mask=df[df["MobileNo"]!=""]
    if not df_by_mobile_mask.empty:
        df_by_mobile_mask["MobileNo"]=df_by_mobile_mask["MobileNo"].str.replace(' ', '')
        df_by_mobile_mask=df_by_mobile_mask[df_by_mobile_mask["MobileNo"]!="0000"]
        df_by_mobile=df_by_mobile_mask.groupby(["MobileNo"],as_index=True).agg(total=("Total","sum"),count=("Total","count"),filterName=("MobileNo","first")).sort_values(by=["total"],ascending=True).reset_index(drop=True)[-10:]
    
    df_by_vehicle_mask=df[df["VehicleNo"]!=""]
    if not df_by_vehicle_mask.empty:
        df_by_vehicle_mask["VehicleNo"]=df_by_vehicle_mask["VehicleNo"].str.replace(' ', '').str.upper()
        df_by_vehicle_mask=df_by_vehicle_mask[df_by_vehicle_mask["VehicleNo"]!="XXX"]
        # Group the data by vehicle number and collect mobile numbers as lists
        # grouped = df.groupby('VehicleNo')['MobileNo'].apply(list)

        # # Filter groups where there are multiple mobile numbers
        # vehicles_with_multiple_mobiles = grouped[grouped.apply(len) > 1]

        # for vehicle_number, mobile_numbers in vehicles_with_multiple_mobiles.items():
        #     print(f"Vehicle {vehicle_number} has multiple mobile numbers: {', '.join(mobile_numbers)}")
        


        df_by_vehicle=df_by_vehicle_mask.groupby(["VehicleNo"],as_index=True).agg(total=("Total","sum"),count=("Total","count"),filterName=("VehicleNo","first")).sort_values(by=["total"],ascending=True).reset_index(drop=True)[-10:]
    
    return {"byName":df_by_name.to_dict('records'),"byMobile":df_by_mobile.to_dict('records'),"byVehicle":df_by_vehicle.to_dict('records')}

def total_sales_based_on_customer(office_id,is_admin,from_date,to_date,cnxn):
    sales_based_on_customer=[]


    if is_admin==6:
        df=godown_list(office_id,from_date,to_date,-1,cnxn)
        sales_based_on_customer=total_sales_based_on_customer_body(df)


    elif is_admin==4:
        df=godown_list(office_id,from_date,to_date,-1,cnxn)
        df=df[~((df["officeType"]!="Company")& (df["masterOfficeId"].str.lower()==office_id.lower()))]
        sales_based_on_customer=total_sales_based_on_customer_body(df)

    elif is_admin==5:
        df=godown_list(office_id,from_date,to_date,-1,cnxn)
        sales_based_on_customer=total_sales_based_on_customer_body(df)

    elif is_admin==1:
        df=godown_list(office_id,from_date,to_date,1,cnxn)
        sales_based_on_customer=total_sales_based_on_customer_body(df[(df["officeType"]=="Wholesale Pumps")| (df["officeType"]=="Retail Pumps")])

    elif is_admin==3:
        df=godown_list(office_id,from_date,to_date,1,cnxn)
    
        df=df[df["officeType"]=="Wholesale Pumps"]
        sales_based_on_customer=total_sales_based_on_customer_body(df)

    elif is_admin==2:
        df=godown_list(office_id,from_date,to_date,1,cnxn)
        df=df[df["officeType"]=="Retail Pumps"]
        sales_based_on_customer=total_sales_based_on_customer_body(df)

    elif is_admin==0:
        df=godown_list(office_id,from_date,to_date,1,cnxn)
        df=df[df["officeId"].str.lower()==office_id.lower()]
        sales_based_on_customer=total_sales_based_on_customer_body(df)

    return sales_based_on_customer