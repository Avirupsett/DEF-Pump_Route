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
    ot.color As officeTypeColor,
    ct.level,
    ot.OfficeTypeName As officeType,
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
    ({level} < 0 OR ct.Level <= {level})
    ''',cnxn)
  
    return df

def calculate_total_income(master_office_id,df):
    total_income = df[df["masterOfficeId"] == master_office_id]["totalIncome"].sum()
    
    child_offices = df[df["masterOfficeId"] == master_office_id]["officeId"].tolist()
    for child_office in child_offices:
        total_income += calculate_total_income(child_office,df)
    
    return total_income

def total_sales_based_on_office_body(df):
    alldata=[]
    try:
        sums_by_master_office = {}
        if not df.empty:
            df=df.groupby(['officeId'],as_index=False).agg({'officeId':'first','totalIncome':'sum','officeName':'first','officeTypeColor':'first','officeType':'first','level':'first','masterOfficeId':'first'})
            
        # Find top-level masterOfficeIds
        top_level_master_office_ids = df[df["level"] == 1]["officeId"].tolist()

        # Calculate sums recursively for each top-level masterOfficeId
        for master_office_id in top_level_master_office_ids:
            total_sum = calculate_total_income(master_office_id,df)
            sums_by_master_office[master_office_id] = total_sum

        Generated_df=pd.DataFrame.from_dict(sums_by_master_office,orient="index",columns=['totalIncome'])

        alldata= pd.merge(Generated_df, df[["officeId","officeName","officeTypeColor","officeType"]], left_index=True, right_on='officeId').sort_values(by=["officeType","officeName"],ascending=[False,False],key=lambda x:x.str.lower()).to_dict(orient='records')
    except:
        print("Total Sales By Entity Error")
    return alldata


def total_sales(office_id,is_admin,from_date,to_date,cnxn):
    alldata=[]
    

    if is_admin==6:
        df=godown_list(office_id,from_date,to_date,-1,cnxn)
        alldata=total_sales_based_on_office_body(df)
    

    elif is_admin==4:
        df=godown_list(office_id,from_date,to_date,-1,cnxn)
        df=df[~((df["officeType"]!="Company")& (df["masterOfficeId"].str.lower()==office_id.lower()))]
        alldata=total_sales_based_on_office_body(df)
    elif is_admin==5:
        df=godown_list(office_id,from_date,to_date,-1,cnxn)
        alldata=total_sales_based_on_office_body(df)
    elif is_admin==1:
        df=godown_list(office_id,from_date,to_date,1,cnxn)
        df=df[(df["officeType"]=="Wholesale Pumps")| (df["officeType"]=="Retail Pumps")]
        alldata=total_sales_based_on_office_body(df)
    elif is_admin==3:
        df=godown_list(office_id,from_date,to_date,1,cnxn)
        df=df[df["officeType"]=="Wholesale Pumps"]
        alldata=total_sales_based_on_office_body(df)
    elif is_admin==2:
        df=godown_list(office_id,from_date,to_date,1,cnxn)
        df=df[df["officeType"]=="Retail Pumps"]
        alldata=total_sales_based_on_office_body(df)
    elif is_admin==0:
        df=godown_list(office_id,from_date,to_date,1,cnxn)
        df=df[df["officeId"].str.lower()==office_id.lower()]
        for office in (df[df["level"]==0]["officeId"].unique()):
            totalIncome=df[df["officeId"].str.lower()==office.lower()]["totalIncome"].sum()
            alldata.append({
                "officeId":office,
                "officeName":df[df["officeId"].str.lower()==office.lower()]["officeName"].unique()[0],
                "officeTypeColor":df[df["officeId"].str.lower()==office.lower()]["officeTypeColor"].unique()[0],
                "officeType":df[df["officeId"].str.lower()==office.lower()]["officeType"].unique()[0],
                "totalIncome":totalIncome
            })


    return alldata
