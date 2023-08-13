import pandas as pd

def godown_list(office_id,from_date,to_date,level,cnxn):
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
	S.totalIncome,
	S.Quantity,
	S.FuelRateId,
	FR.ProductTypeId As productId,
	Pt.ProductTypeName As productName,
	UM.UnitName As unitName,
	UM.UnitShortName As unitShortName,
	Um.SingularShortName As singularShortName,
    S.Rate As rate,
    PT.Color As color
	
FROM cte_org ct
LEFT OUTER JOIN OfficeType ot ON ct.OfficeTypeId = ot.OfficeTypeId

Left Outer join
(
Select SUM(Total) As totalIncome,Sum(Quantity) As Quantity,InvoiceDate,FuelRateId,OfficeId,Rate
From Sales
Where
IsDeleted=0 AND
InvoiceDate>='{from_date}' AND InvoiceDate<='{to_date}'
Group By
InvoiceDate,FuelRateId,OfficeId,Rate
)S On ct.officeId=S.OfficeId 

Left join
FuelRate FR ON FR.FuelRateId=S.FuelRateId
Left join
ProductType PT ON FR.ProductTypeId=PT.ProductTypeId
Left join
UnitMaster UM ON PT.PrimaryUnitId=UM.UnitId
WHERE
    ct.IsActive=1 and ({level} < 0 OR ct.Level <= {level})
    ''',cnxn)
    Expense_df2=pd.read_sql_query(f'''
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
    ct.OfficeId As officeId,
    ct.OfficeName As officeName,
    ot.OfficeTypeName As officeType,
	E.VoucherDate As expenseDate,
	E.totalExpense
	
FROM cte_org ct
LEFT OUTER JOIN OfficeType ot ON ct.OfficeTypeId = ot.OfficeTypeId

Left Outer join
(
Select officeId,SUM(Amount) As totalExpense,VoucherDate
From Expense
Where
IsDeleted=0 AND
VoucherDate>='{from_date}' AND VoucherDate<='{to_date}'
Group By
officeId, VoucherDate
)E On ct.officeId=E.OfficeId

WHERE
   ct.IsActive=1 and ({level} < 0 OR ct.Level <= {level})
    ''',cnxn)
    Customer_df3=pd.read_sql_query(f'''
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
   ct.IsActive=1 and ({level} < 0 OR ct.Level <= {level})
    ''',cnxn)
    return Sales_df1,Expense_df2,Customer_df3

def total_sales_based_on_customer_body(df):
    df_by_name=pd.DataFrame()
    df_by_mobile=pd.DataFrame()
    df_by_vehicle=pd.DataFrame()
    
    try:
        df_by_name_mask=df[df["CustomerName"]!=""]
        if not df_by_name_mask.empty:
            df_by_name_mask["CustomerName"]=df_by_name_mask["CustomerName"].str.upper()
            df_by_name_mask=df_by_name_mask[df_by_name_mask["CustomerName"]!="XXX"]
            df_by_name=df_by_name_mask.groupby(["CustomerName"],as_index=True).agg(total=("Total","sum"),count=("Total","count"),filterName=("CustomerName","first")).sort_values(by=["total"],ascending=True).reset_index(drop=True)[-10:]
    except:
        print("No CustomerName Data Found")

    try:
        df_by_mobile_mask=df[df["MobileNo"]!=""]
        if not df_by_mobile_mask.empty:
            df_by_mobile_mask["MobileNo"]=df_by_mobile_mask["MobileNo"].str.replace(' ', '')
            df_by_mobile_mask=df_by_mobile_mask[df_by_mobile_mask["MobileNo"]!="0000"]
            df_by_mobile=df_by_mobile_mask.groupby(["MobileNo"],as_index=True).agg(total=("Total","sum"),count=("Total","count"),filterName=("MobileNo","first")).sort_values(by=["total"],ascending=True).reset_index(drop=True)[-10:]
        
    except:
        print("No MobileNo Data Found")

    try:
        df_by_vehicle_mask=df[df["VehicleNo"]!=""]
        if not df_by_vehicle_mask.empty:
            df_by_vehicle_mask["VehicleNo"]=df_by_vehicle_mask["VehicleNo"].str.replace(' ', '').str.upper()
            df_by_vehicle_mask=df_by_vehicle_mask[df_by_vehicle_mask["VehicleNo"]!="XXX"]
            df_by_vehicle=df_by_vehicle_mask.groupby(["VehicleNo"],as_index=True).agg(total=("Total","sum"),count=("Total","count"),filterName=("VehicleNo","first")).sort_values(by=["total"],ascending=True).reset_index(drop=True)[-10:]
        
    except:
        print("No VehicleNo Data Found")
        # Group the data by vehicle number and collect mobile numbers as lists
        # grouped = df.groupby('VehicleNo')['MobileNo'].apply(list)

        # # Filter groups where there are multiple mobile numbers
        # vehicles_with_multiple_mobiles = grouped[grouped.apply(len) > 1]

        # for vehicle_number, mobile_numbers in vehicles_with_multiple_mobiles.items():
        #     print(f"Vehicle {vehicle_number} has multiple mobile numbers: {', '.join(mobile_numbers)}")
        


    
    return {"byName":df_by_name.to_dict('records'),"byMobile":df_by_mobile.to_dict('records'),"byVehicle":df_by_vehicle.to_dict('records')}

def total_sales_based_on_office_body(df):
    alldata=[]
    for office in (df[df["level"]==1]["officeId"].unique()):
            if(len(df[df["masterOfficeId"].str.lower()==office.lower()])):
                totalIncome=df[df["masterOfficeId"].str.lower()==office.lower()]["totalIncome"].sum()
                for innerOffice in df[df["masterOfficeId"].str.lower()==office.lower()]["officeId"].unique():
                    totalIncome+=df[df["masterOfficeId"].str.lower()==innerOffice.lower()]["totalIncome"].sum()
                alldata.append({
                    "officeId":office,
                    "officeName":df[df["masterOfficeId"].str.lower()==office.lower()]["masterOfficeName"].unique()[0],
                    "officeTypeColor":df[df["officeId"].str.lower()==office.lower()]["officeTypeColor"].unique()[0],
                    "officeType":df[df["officeId"].str.lower()==office.lower()]["officeType"].unique()[0],
                    "totalIncome":totalIncome
                })
            else:
                totalIncome=df[df["officeId"].str.lower()==office.lower()]["totalIncome"].sum()
                alldata.append({
                    "officeId":office,
                    "officeName":df[df["officeId"].str.lower()==office.lower()]["officeName"].unique()[0],
                    "officeTypeColor":df[df["officeId"].str.lower()==office.lower()]["officeTypeColor"].unique()[0],
                    "officeType":df[df["officeId"].str.lower()==office.lower()]["officeType"].unique()[0],
                    "totalIncome":totalIncome
                })
    final_df=pd.DataFrame(alldata)
    try:
        final_df=(final_df.sort_values(by=["officeType","officeName"],ascending=[False,False],key=lambda x:x.str.lower())).reset_index(drop=True)
    except:
        print("No Data Found")

    return final_df.to_dict('records')

def sales_based_on_admin_body(date_range,df1,df2):
    alldata=[]
    productdata=[]
    for i in date_range:
        income=df1[df1["incomeDate"]==i]["totalIncome"].sum()
        expense=df2[df2["expenseDate"]==i]["totalExpense"].sum()
        try:
            Product_list=df1[(df1["incomeDate"]==i)].groupby(["productId","rate"]).agg({"totalIncome":"sum","Quantity":"sum","productName":"first","unitName":"first","unitShortName":"first","singularShortName":"first","color":"first"}).reset_index()[["productId","productName","unitName","unitShortName","singularShortName","totalIncome","Quantity","rate","color"]]
        except:
            Product_list=pd.DataFrame()
        # try:
        #     Sales_list=df1[(df1["incomeDate"]==i)].groupby(["officeId"]).agg({"totalIncome":"sum","officeName":"first","officeType":"first"}).reset_index()[["officeId","officeName","officeType","totalIncome"]]
        # except:
        #     Sales_list=pd.DataFrame()
        # try:
        #     Expense_list=df2[(df2["expenseDate"]==i)].groupby(["officeId"]).agg({"totalExpense":"sum","officeName":"first","officeType":"first"}).reset_index()[["officeId","officeName","officeType","totalExpense"]]
        # except:
        #     Expense_list=pd.DataFrame()
        # try:
        #     Merged_list=pd.merge(Sales_list,Expense_list,on=["officeId","officeName","officeType"],how="outer").fillna(0)
        # except:
        #     Merged_list=pd.DataFrame()
        try:
            Product_list.rename({"totalIncome":"totalSales","Quantity":"qty"},axis=1,inplace=True)
            Product_list=Product_list.astype({"totalSales":int,"qty":int,"productId":int})
        except:
            Product_list=pd.DataFrame()
                
        alldata.append({
            "requestedDate": pd.to_datetime(i).strftime("%Y-%m-%d"),
            "totalIncome": income,
            "totalExpense": expense,
            # "lstOffice":Merged_list.to_dict(orient="records")
        })
        productdata.append({
            "requestedDate": pd.to_datetime(i).strftime("%Y-%m-%d"),
            "lstproduct": Product_list.to_dict(orient="records")
            
        })
    return alldata,productdata

def sales_based_on_admin(office_id,is_admin,from_date,to_date,cnxn):
    
    
    sales_based_on_date=[]
    sales_based_on_product=[]
    sales_based_on_office=[]
    sales_based_on_customer=[]

    if is_admin==6:
        date_range=pd.date_range(from_date,to_date)
        df1,df2,df3=godown_list(office_id,from_date,to_date,-1,cnxn)
        sales_based_on_date,sales_based_on_product=sales_based_on_admin_body(date_range,df1,df2)
        sales_based_on_office=total_sales_based_on_office_body(df1)
        sales_based_on_customer=total_sales_based_on_customer_body(df3)


    elif is_admin==4:
        date_range=pd.date_range(from_date,to_date)
        df1,df2,df3=godown_list(office_id,from_date,to_date,-1,cnxn)
        df1=df1[~((df1["officeType"]!="Company")& (df1["masterOfficeId"].str.lower()==office_id.lower()))]
        df2=df2[~((df2["officeType"]!="Company")& (df2["masterOfficeId"].str.lower()==office_id.lower()))]
        df3=df3[~((df2["officeType"]!="Company")& (df3["masterOfficeId"].str.lower()==office_id.lower()))]
        sales_based_on_date,sales_based_on_product=sales_based_on_admin_body(date_range,df1,df2)
        sales_based_on_office=total_sales_based_on_office_body(df1)
        sales_based_on_customer=total_sales_based_on_customer_body(df3)

    elif is_admin==5:
        date_range=pd.date_range(from_date,to_date)
        df1,df2,df3=godown_list(office_id,from_date,to_date,-1,cnxn)
        sales_based_on_date,sales_based_on_product=sales_based_on_admin_body(date_range,df1,df2)
        sales_based_on_office=total_sales_based_on_office_body(df1)
        sales_based_on_customer=total_sales_based_on_customer_body(df3)

    elif is_admin==1:
        date_range=pd.date_range(from_date,to_date)
        df1,df2,df3=godown_list(office_id,from_date,to_date,1,cnxn)
        # if(df1[df1["officeId"].str.lower()==office_id.lower()]["officeType"].values[0]=="Company" or df2[df2["officeId"].str.lower()==office_id.lower()]["officeType"].values[0]=="Company"):
        sales_based_on_date,sales_based_on_product=sales_based_on_admin_body(date_range,df1,df2)
        sales_based_on_office=total_sales_based_on_office_body(df1[(df1["officeType"]=="Wholesale Pumps")| (df1["officeType"]=="Retail Pumps")])
        sales_based_on_customer=total_sales_based_on_customer_body(df3[(df3["officeType"]=="Wholesale Pumps")| (df3["officeType"]=="Retail Pumps")])

    elif is_admin==3:
        date_range=pd.date_range(from_date,to_date)
        df1,df2,df3=godown_list(office_id,from_date,to_date,1,cnxn)
        # if(df1[df1["officeId"].str.lower()==office_id.lower()]["officeType"].values[0]=="Company" or df2[df2["officeId"].str.lower()==office_id.lower()]["officeType"].values[0]=="Company"):
        df1=df1[df1["officeType"]=="Wholesale Pumps"]
        df2=df2[df2["officeType"]=="Wholesale Pumps"]
        df3=df3[df3["officeType"]=="Wholesale Pumps"]
        sales_based_on_date,sales_based_on_product=sales_based_on_admin_body(date_range,df1,df2)
        sales_based_on_office=total_sales_based_on_office_body(df1)
        sales_based_on_customer=total_sales_based_on_customer_body(df3)

    elif is_admin==2:
        date_range=pd.date_range(from_date,to_date)
        df1,df2,df3=godown_list(office_id,from_date,to_date,1,cnxn)
        # if(df1[df1["officeId"].str.lower()==office_id.lower()]["officeType"].values[0]=="Company" or df2[df2["officeId"].str.lower()==office_id.lower()]["officeType"].values[0]=="Company"):
        df1=df1[df1["officeType"]=="Retail Pumps"]
        df2=df2[df2["officeType"]=="Retail Pumps"]
        df3=df3[df3["officeType"]=="Retail Pumps"]
        sales_based_on_date,sales_based_on_product=sales_based_on_admin_body(date_range,df1,df2)
        sales_based_on_office=total_sales_based_on_office_body(df1)
        sales_based_on_customer=total_sales_based_on_customer_body(df3)

    elif is_admin==0:
        date_range=pd.date_range(from_date,to_date)
        df1,df2,df3=godown_list(office_id,from_date,to_date,1,cnxn)
        df1=df1[df1["officeId"].str.lower()==office_id.lower()]
        df2=df2[df2["officeId"].str.lower()==office_id.lower()]
        df3=df3[df3["officeId"].str.lower()==office_id.lower()]
        for i in date_range:
            income=df1[df1["incomeDate"]==i]["totalIncome"].sum()
            expense=df2[df2["expenseDate"]==i]["totalExpense"].sum()
           
            Product_list=df1[(df1["incomeDate"]==i)].groupby(["productId","productName","unitName","unitShortName","singularShortName","rate","color"]).agg({"totalIncome":"sum","Quantity":"sum"}).reset_index()[["productId","productName","unitName","unitShortName","singularShortName","totalIncome","Quantity","rate","color"]]
            
            Product_list.rename({"totalIncome":"totalSales","Quantity":"qty"},axis=1,inplace=True)
            Product_list=Product_list.astype({"totalSales":int,"qty":int,"productId":int})
            
            
            sales_based_on_date.append({
                "requestedDate": pd.to_datetime(i).strftime("%Y-%m-%d"),
                "totalIncome": income,
                "totalExpense": expense,
                "lstOffice":[]
            })
            sales_based_on_product.append({
                "requestedDate": pd.to_datetime(i).strftime("%Y-%m-%d"),
                "lstproduct": Product_list.to_dict(orient="records")
                
            })
        for office in (df1[df1["level"]==0]["officeId"].unique()):
            totalIncome=df1[df1["officeId"].str.lower()==office.lower()]["totalIncome"].sum()
            sales_based_on_office.append({
                "officeId":office,
                "officeName":df1[df1["officeId"].str.lower()==office.lower()]["officeName"].unique()[0],
                "officeTypeColor":df1[df1["officeId"].str.lower()==office.lower()]["officeTypeColor"].unique()[0],
                "officeType":df1[df1["officeId"].str.lower()==office.lower()]["officeType"].unique()[0],
                "totalIncome":totalIncome
            })
        sales_based_on_customer=total_sales_based_on_customer_body(df3)


    return sales_based_on_date,sales_based_on_product,sales_based_on_office,sales_based_on_customer
