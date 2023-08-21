import pandas as pd
from controllers.Dashboard.Sales.sales_list_functions.paymentMode import paymentMode_Body
from controllers.Dashboard.Sales.sales_list_functions.salesCustomer import total_sales_based_on_customer_body
from controllers.Dashboard.Sales.sales_list_functions.salesOffice import total_sales_based_on_office_body
from controllers.Dashboard.Sales.sales_list_functions.salesExpense import sales_based_on_admin_body
# import time

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
	FR.ProductTypeId As productId,
	Pt.ProductTypeName As productName,
	UM.UnitName As unitName,
	UM.UnitShortName As unitShortName,
	Um.SingularShortName As singularShortName,
    S.Rate As rate,
    PT.Color As color,
    S.CustomerName,
    S.MobileNo,
    S.VehicleNo,
    PM.PaymentModeName
	
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
Left Join
	PaymentModeMaster PM ON PM.PaymentModeId=S.PaymentModeId
Left join
FuelRate FR ON FR.FuelRateId=S.FuelRateId
Left join
ProductType PT ON FR.ProductTypeId=PT.ProductTypeId
Left join
UnitMaster UM ON PT.PrimaryUnitId=UM.UnitId
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


def sales_based_on_admin(office_id,is_admin,from_date,to_date,cnxn):
       
    sales_based_on_date=[]
    sales_based_on_product=[]
    sales_based_on_office=[]
    sales_based_on_customer=[]
    paymentMode=[]
    # start_time=time.time()
    if is_admin==6:
        date_range=pd.date_range(from_date,to_date)
        df1=Sales_Traverse_list(office_id,from_date,to_date,-1,cnxn)
        df2=Expense_Traverse_list(office_id,from_date,to_date,-1,cnxn)
        sales_based_on_date,sales_based_on_product=sales_based_on_admin_body(date_range,df1,df2)
        sales_based_on_office=total_sales_based_on_office_body(df1)
        sales_based_on_customer=total_sales_based_on_customer_body(df1)
        paymentMode=paymentMode_Body(df1,cnxn)


    elif is_admin==4:
        date_range=pd.date_range(from_date,to_date)
        df1=Sales_Traverse_list(office_id,from_date,to_date,-1,cnxn)
        df2=Expense_Traverse_list(office_id,from_date,to_date,-1,cnxn)
        df1=df1[~((df1["officeType"]!="Company")& (df1["masterOfficeId"].str.lower()==office_id.lower()))]
        df2=df2[~((df2["officeType"]!="Company")& (df2["masterOfficeId"].str.lower()==office_id.lower()))]
        sales_based_on_date,sales_based_on_product=sales_based_on_admin_body(date_range,df1,df2)
        sales_based_on_office=total_sales_based_on_office_body(df1)
        sales_based_on_customer=total_sales_based_on_customer_body(df1)
        paymentMode=paymentMode_Body(df1,cnxn)

    elif is_admin==5:
        date_range=pd.date_range(from_date,to_date)
        df1=Sales_Traverse_list(office_id,from_date,to_date,-1,cnxn)
        df2=Expense_Traverse_list(office_id,from_date,to_date,-1,cnxn)
        sales_based_on_date,sales_based_on_product=sales_based_on_admin_body(date_range,df1,df2)
        sales_based_on_office=total_sales_based_on_office_body(df1)
        sales_based_on_customer=total_sales_based_on_customer_body(df1)
        paymentMode=paymentMode_Body(df1,cnxn)

    elif is_admin==1:
        date_range=pd.date_range(from_date,to_date)
        df1=Sales_Traverse_list(office_id,from_date,to_date,1,cnxn)
        df2=Expense_Traverse_list(office_id,from_date,to_date,1,cnxn)
        # if(df1[df1["officeId"].str.lower()==office_id.lower()]["officeType"].values[0]=="Company" or df2[df2["officeId"].str.lower()==office_id.lower()]["officeType"].values[0]=="Company"):
        sales_based_on_date,sales_based_on_product=sales_based_on_admin_body(date_range,df1,df2)
        df1=df1[(df1["officeType"]=="Wholesale Pumps")| (df1["officeType"]=="Retail Pumps")]
        sales_based_on_office=total_sales_based_on_office_body(df1)
        sales_based_on_customer=total_sales_based_on_customer_body(df1)
        paymentMode=paymentMode_Body(df1,cnxn)

    elif is_admin==3:
        date_range=pd.date_range(from_date,to_date)
        df1=Sales_Traverse_list(office_id,from_date,to_date,1,cnxn)
        df2=Expense_Traverse_list(office_id,from_date,to_date,1,cnxn)
        # if(df1[df1["officeId"].str.lower()==office_id.lower()]["officeType"].values[0]=="Company" or df2[df2["officeId"].str.lower()==office_id.lower()]["officeType"].values[0]=="Company"):
        df1=df1[df1["officeType"]=="Wholesale Pumps"]
        df2=df2[df2["officeType"]=="Wholesale Pumps"]
        sales_based_on_date,sales_based_on_product=sales_based_on_admin_body(date_range,df1,df2)
        sales_based_on_office=total_sales_based_on_office_body(df1)
        sales_based_on_customer=total_sales_based_on_customer_body(df1)
        paymentMode=paymentMode_Body(df1,cnxn)

    elif is_admin==2:
        date_range=pd.date_range(from_date,to_date)
        df1=Sales_Traverse_list(office_id,from_date,to_date,1,cnxn)
        df2=Expense_Traverse_list(office_id,from_date,to_date,1,cnxn)
        # if(df1[df1["officeId"].str.lower()==office_id.lower()]["officeType"].values[0]=="Company" or df2[df2["officeId"].str.lower()==office_id.lower()]["officeType"].values[0]=="Company"):
        df1=df1[df1["officeType"]=="Retail Pumps"]
        df2=df2[df2["officeType"]=="Retail Pumps"]
        sales_based_on_date,sales_based_on_product=sales_based_on_admin_body(date_range,df1,df2)
        sales_based_on_office=total_sales_based_on_office_body(df1)
        sales_based_on_customer=total_sales_based_on_customer_body(df1)
        paymentMode=paymentMode_Body(df1,cnxn)

    elif is_admin==0:
        date_range=pd.date_range(from_date,to_date)
        df1=Sales_Traverse_list(office_id,from_date,to_date,1,cnxn)
        df2=Expense_Traverse_list(office_id,from_date,to_date,1,cnxn)
        df1=df1[df1["officeId"].str.lower()==office_id.lower()]
        df2=df2[df2["officeId"].str.lower()==office_id.lower()]

        sales_based_on_date,sales_based_on_product=sales_based_on_admin_body(date_range,df1,df2)

        for office in (df1[df1["level"]==0]["officeId"].unique()):
            totalIncome=df1[df1["officeId"].str.lower()==office.lower()]["totalIncome"].sum()
            sales_based_on_office.append({
                "officeId":office,
                "officeName":df1[df1["officeId"].str.lower()==office.lower()]["officeName"].unique()[0],
                "officeTypeColor":df1[df1["officeId"].str.lower()==office.lower()]["officeTypeColor"].unique()[0],
                "officeType":df1[df1["officeId"].str.lower()==office.lower()]["officeType"].unique()[0],
                "totalIncome":totalIncome
            })
        sales_based_on_customer=total_sales_based_on_customer_body(df1)
        paymentMode=paymentMode_Body(df1,cnxn)
    
    # print("Final Results: ",time.time()-start_time)


    return sales_based_on_date,sales_based_on_product,sales_based_on_office,sales_based_on_customer,paymentMode
