import pandas as pd
from flask import jsonify

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
    ANR.Name As Name
	
FROM cte_org ct
LEFT OUTER JOIN OfficeType ot ON ct.OfficeTypeId = ot.OfficeTypeId

Left Outer Join
UserOfficeMapper UM ON ct.OfficeId=UM.OfficeId

Left Join
AspNetUserRoles ANUR ON ANUR.userId=UM.userId

Left Join
AspNetRoles ANR ON ANR.Id=ANUR.RoleId


WHERE
    ct.IsActive=1 and ({level} < 0 OR ct.Level <= {level}) 
""",
        cnxn,
    )

    return df

def CardDetails(office_id,is_admin,cnxn):

    today_date = pd.to_datetime('today').date()
    previous_date = today_date
    previous_7_date = today_date - pd.DateOffset(days=7)
    incomeDetailsCurrentDay_total=0
    incomeDetailsCurrentDay_count=0
    expenseDetailsCurrentDay_total=0
    expenseDetailsCurrentDay_count=0
    userCount=pd.DataFrame()
    
    if(is_admin==1):
        df = CardDetails_level(office_id,previous_7_date,previous_date,-1,cnxn)
        CurrentDay_df = CardDetails_level_CurrentDay(office_id,today_date,-1,cnxn)
        User_df=CardDetails_user(office_id,1,cnxn)
        User_df=User_df[User_df["level"]>0]

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
            }
        }
    )
