import pandas as pd
from flask import jsonify

def Sales_Traverse_list(office_id,from_date,to_date,level,cnxn):
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
	S.Quantity,
	S.FuelRateId,
    S.Rate As rate,
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
def Expense_Traverse_list(office_id,from_date,to_date,level,cnxn):
    # start_time=time.time()
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

    # print("Expense Query Time: ",time.time()-start_time)
    return Expense_df2

def sales_based_on_admin_body(date_range,df1,df2):
    alldata=[]
    # start_time=time.time()
    try:

        if not df1.empty:
            Sales_result = df1.groupby("incomeDate").apply(lambda group: {
                "date": group["incomeDate"].iloc[0].strftime("%Y-%m-%d"),
                "totalIncome": group["totalIncome"].sum()})
        else:
            Sales_result=[]

        if not df2.empty:
            df2["expenseDate"]=pd.to_datetime(df2["expenseDate"])
            Expense_result = df2.groupby("expenseDate").apply(lambda group: {
                "date": group["expenseDate"].iloc[0].strftime("%Y-%m-%d"),
                "totalExpense": group["totalExpense"].sum()})
        else:
            Expense_result = []
        
        for i in date_range:
            
            alldata.append({
                "requestedDate": pd.to_datetime(i).strftime("%Y-%m-%d"),
                "totalIncome": Sales_result[pd.to_datetime(i).strftime("%Y-%m-%d")]["totalIncome"] if pd.to_datetime(i).strftime("%Y-%m-%d") in Sales_result else 0,
                "totalExpense": Expense_result[pd.to_datetime(i).strftime("%Y-%m-%d")]["totalExpense"] if pd.to_datetime(i).strftime("%Y-%m-%d") in Expense_result else 0,
                # "lstOffice":Merged_list.to_dict(orient="records")
            })
      
    except:
        print("Sales Expense Error")

    # print("Graph1 and Graph2: ",time.time()-start_time)
    
    return alldata

def CardDetails_level(office_id,from_date,to_date,level,cnxn):
    df = pd.read_sql_query(
        f"""
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
    ct.Level as level,
    ct.MasterOfficeId As masterOfficeId,
    ct.OfficeId As officeId,
    ct.OfficeName As officeName,
    ot.OfficeTypeName As officeType,
	S.Total,S.IncomeCount,
    E.Amount,E.ExpenseCount
	
FROM cte_org ct
LEFT OUTER JOIN OfficeType ot ON ct.OfficeTypeId = ot.OfficeTypeId

Left Outer join
(
Select SUM(Total) AS Total,Count(Total) AS IncomeCount,OfficeId
From Sales
Where
IsDeleted=0 And
InvoiceDate>='{from_date}' AND InvoiceDate<='{to_date}'
GROUP BY
OfficeId
)S On ct.officeId=S.OfficeId 

Left Outer join
(
Select SUM(Amount) AS Amount,Count(Amount) AS ExpenseCount,OfficeId
From Expense
Where
VoucherDate>='{from_date}' AND VoucherDate<='{to_date}'
GROUP BY
OfficeId
)E On ct.officeId=E.OfficeId


WHERE
    ct.IsActive=1 and ({level} < 0 OR ct.Level <= {level}) 
""",
        cnxn,
    )

    return df

def CardDetails_level_CurrentDay(office_id,date,level,cnxn):
    df = pd.read_sql_query(
        f"""
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
    ct.Level as level,
    ct.MasterOfficeId As masterOfficeId,
    ct.OfficeId As officeId,
    ct.OfficeName As officeName,
    ot.OfficeTypeName As officeType,
	S.Total,S.IncomeCount,
    E.Amount,E.ExpenseCount
	
FROM cte_org ct
LEFT OUTER JOIN OfficeType ot ON ct.OfficeTypeId = ot.OfficeTypeId

Left Outer join
(
Select SUM(Total) AS Total,Count(Total) AS IncomeCount,OfficeId
From Sales
Where
IsDeleted=0 And
InvoiceDate='{date}'
GROUP BY
OfficeId
)S On ct.officeId=S.OfficeId 

Left Outer join
(
Select SUM(Amount) AS Amount,Count(Amount) AS ExpenseCount,OfficeId
From Expense
Where
VoucherDate='{date}'
GROUP BY
OfficeId
)E On ct.officeId=E.OfficeId


WHERE
    ct.IsActive=1 and ({level} < 0 OR ct.Level <= {level}) 
""",
        cnxn,
    )

    return df

def CardDetails_user(office_id,level,cnxn):
    df = pd.read_sql_query(
        f"""
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
    ct.Level as level,
    ct.MasterOfficeId As masterOfficeId,
    ct.OfficeId As officeId,
    ct.OfficeName As officeName,
    ot.OfficeTypeName As officeType,
    ANR.Name As Name,
    ANUR.userId
	
FROM cte_org ct
LEFT OUTER JOIN OfficeType ot ON ct.OfficeTypeId = ot.OfficeTypeId

Left Outer Join
UserOfficeMapper UM ON ct.OfficeId=UM.OfficeId

Left Join
AspNetUserRoles ANUR ON ANUR.userId=UM.userId

Left Join
AspNetRoles ANR ON ANR.Id=ANUR.RoleId


WHERE
   UM.IsActive=1 and ct.IsActive=1 and ({level} < 0 OR ct.Level <= {level}) 
""",
        cnxn,
    )

    return df

def CardDetails(office_id,is_admin,cnxn):

    today_date = pd.to_datetime('today').date()
    previous_date = today_date
    previous_7_date = today_date - pd.DateOffset(days=6)
    previous_14_date = today_date - pd.DateOffset(days=13)
    incomeDetailsCurrentDay_total=0
    incomeDetailsCurrentDay_count=0
    expenseDetailsCurrentDay_total=0
    expenseDetailsCurrentDay_count=0
    userCount=pd.DataFrame()
    sales_based_on_date=[]
    
    if(is_admin==1):
        df = CardDetails_level(office_id,previous_7_date,previous_date,-1,cnxn)
        CurrentDay_df = CardDetails_level_CurrentDay(office_id,today_date,-1,cnxn)
        User_df=CardDetails_user(office_id,1,cnxn)
        User_df=User_df[(User_df["level"]>0)&~(User_df["Name"].str.lower()=="superadmin")]

        df1=df.copy()

        df1=df1[~((df1["level"]>1)|(df1["level"]==0)|(df1["officeId"].str.lower()==office_id.lower()))]

        Sales_Expense_df=df1[["officeId","officeType"]].drop_duplicates()
        if not Sales_Expense_df.empty:
            officeCount=Sales_Expense_df.groupby('officeType',as_index=False).agg(officeCount=('officeType','count'))
        else:
            officeCount=pd.DataFrame()

        if not CurrentDay_df.empty:
            incomeDetailsCurrentDay_total=CurrentDay_df["Total"].sum()
            incomeDetailsCurrentDay_count=CurrentDay_df["IncomeCount"].sum()
            expenseDetailsCurrentDay_total=CurrentDay_df["Amount"].sum()
            expenseDetailsCurrentDay_count=CurrentDay_df["ExpenseCount"].sum()

        # User_df=User_df[["Name","officeId"]]
        # User_df=User_df[~((df1["Name"]==0)|(df["officeId"]==0))]
        userCount=User_df.groupby("Name",as_index=False).agg(userCount=("Name","count")).rename(columns={"Name":"roleName"})
        # userCount=pd.DataFrame.from_dict(userCount,orient='index',columns=["userCount"])
        # userCount.reset_index(inplace=True,drop=False)
        # userCount.rename(columns={'index':'roleName'},inplace=True)

        if(len(userCount)>0 or len(officeCount)>0):
            Sales_df1=Sales_Traverse_list(office_id,previous_14_date,today_date,-1,cnxn)
            Expense_df2=Expense_Traverse_list(office_id,previous_14_date,today_date,-1,cnxn)
            sales_based_on_date=sales_based_on_admin_body(pd.date_range(previous_14_date,today_date),Sales_df1,Expense_df2)
        else:
            Sales_df1=Sales_Traverse_list(office_id,previous_14_date,today_date,0,cnxn)
            Expense_df2=Expense_Traverse_list(office_id,previous_14_date,today_date,0,cnxn)
            sales_based_on_date=sales_based_on_admin_body(pd.date_range(previous_14_date,today_date),Sales_df1,Expense_df2)
        
    else:
        df = CardDetails_level(office_id,previous_7_date,previous_date,0,cnxn)
        CurrentDay_df = CardDetails_level_CurrentDay(office_id,today_date,0,cnxn)
        User_df=CardDetails_user(office_id,0,cnxn)
        User_df=User_df[~(User_df["Name"].str.lower()=="superadmin")]
        df2 = df.copy()
        Sales_Expense_df=df2[["officeId","officeType"]].drop_duplicates()

        if not Sales_Expense_df.empty:
            officeCount=Sales_Expense_df.groupby('officeType',as_index=False).agg(officeCount=('officeType','count'))
        else:
            officeCount=pd.DataFrame()

        if not CurrentDay_df.empty:
            incomeDetailsCurrentDay_total=CurrentDay_df["Total"].sum()
            incomeDetailsCurrentDay_count=CurrentDay_df["IncomeCount"].sum()
            expenseDetailsCurrentDay_total=CurrentDay_df["Amount"].sum()
            expenseDetailsCurrentDay_count=CurrentDay_df["ExpenseCount"].sum()
        # officeCount=pd.DataFrame.from_dict(officeCount,orient='index',columns=['officeCount'])
        # officeCount.reset_index(inplace=True,drop=False)
        # officeCount.rename(columns={'index':'officeTypeName'},inplace=True)

        # User_df=df2["Name"]
        # userCount=User_df.value_counts().to_dict()
        userCount=User_df.groupby("Name",as_index=False).agg(userCount=("Name","count")).rename(columns={"Name":"roleName"})
        # userCount.reset_index(inplace=True,drop=False)
        # userCount.rename(columns={'index':'roleName'},inplace=True)
        Sales_df1=Sales_Traverse_list(office_id,previous_14_date,today_date,0,cnxn)
        Expense_df2=Expense_Traverse_list(office_id,previous_14_date,today_date,0,cnxn)
        sales_based_on_date=sales_based_on_admin_body(pd.date_range(previous_14_date,today_date),Sales_df1,Expense_df2)
    
  
    incomeDetailsPreviousWeek_total=0
    incomeDetailsPreviousWeek_count=0
    expenseDetailsPreviousWeek_total=0
    expenseDetailsPreviousWeek_count=0
    if(len(df)>0):
        incomeDetailsPreviousWeek_total=df["Total"].sum()
        incomeDetailsPreviousWeek_count=df["IncomeCount"].sum()
        expenseDetailsPreviousWeek_total=df["Amount"].sum()
        expenseDetailsPreviousWeek_count=df["ExpenseCount"].sum()
    
    


    return jsonify(
        {
            "userCount":userCount.to_dict(orient="records"),
            "officeCount":officeCount.to_dict(orient='records'),
            "incomeDetails":{
                "total":int(incomeDetailsPreviousWeek_total),
                "count":int(incomeDetailsPreviousWeek_count)
            },
            "expenseDetails":{
                "total":int(expenseDetailsPreviousWeek_total),
                "count":int(expenseDetailsPreviousWeek_count)
            },
            "incomeDetailsCurrentDay":{
                "total":int(incomeDetailsCurrentDay_total),
                "count":int(incomeDetailsCurrentDay_count)
            },
            "expenseDetailsCurrentDay":{
                "total":int(expenseDetailsCurrentDay_total),
                "count":int(expenseDetailsCurrentDay_count)
            },
            "graph1":sales_based_on_date
        }
    )
