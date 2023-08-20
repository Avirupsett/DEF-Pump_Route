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
	S.total,
    S.CustomerName,
    S.MobileNo,
    S.VehicleNo,
    S.InvoiceDate,
    S.Quantity As qty,
    FR.ProductTypeId As productId,
	Pt.ProductTypeName As productName,
	UM.UnitName As unitName,
	UM.UnitShortName As unitShortName,
	Um.SingularShortName As singularShortName,
    S.Rate As rate,
    PT.Color As color
	
FROM cte_org ct
LEFT OUTER JOIN OfficeType ot ON ct.OfficeTypeId = ot.OfficeTypeId

Left Outer join (
    Select total,
    CustomerName,
    MobileNo,
    VehicleNo,
    OfficeId,
    InvoiceDate,
    Rate,
    FuelRateId,
    Quantity
    From
    Sales Where
IsDeleted=0 AND
InvoiceDate>='{from_date}' AND InvoiceDate<='{to_date}') S
On ct.officeId=S.OfficeId
Left join
FuelRate FR ON FR.FuelRateId=S.FuelRateId
Left join
ProductType PT ON FR.ProductTypeId=PT.ProductTypeId
Left join
UnitMaster UM ON PT.PrimaryUnitId=UM.UnitId

WHERE
    ({level} < 0 OR ct.Level <= {level})
    ''',cnxn)
    return df

def total_sales_based_on_customer_body(df,date_range,CustomerName,MobileNo,VehicleNo):
    
    try:
        alldata=[]
        # date_df = pd.DataFrame({"requestedDate": date_range})
        if CustomerName!=None:
            df=df[df["CustomerName"].str.upper()==CustomerName]
        elif MobileNo!=None:
            df=df[df["MobileNo"].str.replace(' ', '')==MobileNo]
        elif VehicleNo!=None:
            df=df[df["VehicleNo"].str.replace(' ', '').str.upper()==VehicleNo]
        
        # alldata_df = date_df.merge(df, left_on="requestedDate",right_on="InvoiceDate", how="left")
        # alldata_df["requestedDate"] = alldata_df["requestedDate"].dt.strftime('%Y-%m-%d')
        if not df.empty:
            Sales_result = df.groupby("InvoiceDate").apply(lambda group: {
                    # "date": group["InvoiceDate"].iloc[0].strftime("%Y-%m-%d"),
                    "totalIncome": group["total"].sum(),
                    "lstproduct":group.groupby(["productId"]).agg({"total":"sum","qty":"sum","productName":"first","unitName":"first","unitShortName":"first","singularShortName":"first","color":"first"}).reset_index().to_dict(orient="records")})
        else:
            Sales_result=[]
    except:
        print("Sales Customer Details Error")
    for i in date_range:
        alldata.append({
                "requestedDate": pd.to_datetime(i).strftime("%Y-%m-%d"),
                "totalIncome": Sales_result[pd.to_datetime(i).strftime("%Y-%m-%d")]["totalIncome"] if pd.to_datetime(i).strftime("%Y-%m-%d") in Sales_result else 0,
                "lstproduct": Sales_result[pd.to_datetime(i).strftime("%Y-%m-%d")]["lstproduct"] if pd.to_datetime(i).strftime("%Y-%m-%d") in Sales_result else [],
            })
    
    # return pd.DataFrame.from_dict(Sales_result.to_dict(),orient='index').reset_index().rename(columns={'index': 'requestedDate'}).to_dict(orient="records")
    return alldata

def total_sales_based_on_customer(office_id,is_admin,from_date,to_date,cnxn,CustomerName,MobileNo,VehicleNo):
    sales_based_on_customer=[]
    date_range=pd.date_range(from_date,to_date)
    

    if is_admin==6:
        df=godown_list(office_id,from_date,to_date,-1,cnxn)
        sales_based_on_customer=total_sales_based_on_customer_body(df,date_range,CustomerName,MobileNo,VehicleNo)


    elif is_admin==4:
        df=godown_list(office_id,from_date,to_date,-1,cnxn)
        df=df[~((df["officeType"]!="Company")& (df["masterOfficeId"].str.lower()==office_id.lower()))]
        sales_based_on_customer=total_sales_based_on_customer_body(df,date_range,CustomerName,MobileNo,VehicleNo)

    elif is_admin==5:
        df=godown_list(office_id,from_date,to_date,-1,cnxn)
        sales_based_on_customer=total_sales_based_on_customer_body(df,date_range,CustomerName,MobileNo,VehicleNo)

    elif is_admin==1:
        df=godown_list(office_id,from_date,to_date,1,cnxn)
        sales_based_on_customer=total_sales_based_on_customer_body(df[(df["officeType"]=="Wholesale Pumps")| (df["officeType"]=="Retail Pumps")],date_range,CustomerName,MobileNo,VehicleNo)

    elif is_admin==3:
        df=godown_list(office_id,from_date,to_date,1,cnxn)
    
        df=df[df["officeType"]=="Wholesale Pumps"]
        sales_based_on_customer=total_sales_based_on_customer_body(df,date_range,CustomerName,MobileNo,VehicleNo)

    elif is_admin==2:
        df=godown_list(office_id,from_date,to_date,1,cnxn)
        df=df[df["officeType"]=="Retail Pumps"]
        sales_based_on_customer=total_sales_based_on_customer_body(df,date_range,CustomerName,MobileNo,VehicleNo)

    elif is_admin==0:
        df=godown_list(office_id,from_date,to_date,1,cnxn)
        df=df[df["officeId"].str.lower()==office_id.lower()]
        sales_based_on_customer=total_sales_based_on_customer_body(df,date_range,CustomerName,MobileNo,VehicleNo)

    return sales_based_on_customer