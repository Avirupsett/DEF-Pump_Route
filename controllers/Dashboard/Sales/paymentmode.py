import pandas as pd
from flask import jsonify

def paymentMode_level(office_id,from_date,to_date,level,cnxn):
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
	ct.OfficeId,
	ct.IsActive,
	ot.OfficeTypeName As officeType,
        ct.masterOfficeId,
		ct.OfficeName,
		ct.Latitude,
		ct.Longitude,
		PM.PaymentModeName
        
    FROM cte_org ct
    LEFT OUTER JOIN OfficeType ot ON ct.OfficeTypeId = ot.OfficeTypeId
	Left Join
	(
	Select * from Sales
	Where 
	IsDeleted=0 and InvoiceDate>='{from_date}' AND InvoiceDate<='{to_date}' 
	)S ON S.officeId=ct.officeId
	Left Join
	PaymentModeMaster PM ON PM.PaymentModeId=S.PaymentModeId
    WHERE
        ct.IsActive=1 and ({level} < 0 OR ct.Level <= {level})
    ORDER BY
        ct.Level;
''',cnxn)
    
    return df

def paymentMode_Body(df,cnxn):
    payment_df=df.groupby(["PaymentModeName"]).agg(Count=("PaymentModeName","count")).reset_index()
    payment_list=payment_df.to_dict(orient='records')
    df2=pd.read_sql_query(f'''Select * From PaymentModeMaster;''',cnxn)
    new_list=df2.to_dict(orient='records')

    payment_mode_names = [item["PaymentModeName"] for item in payment_list]
    for item in new_list:
        
        if item["PaymentModeName"] not in payment_mode_names:
            payment_list.append({"PaymentModeName":item["PaymentModeName"],"Count":0})

    return jsonify(payment_list)

def paymentMode(office_id,is_admin,from_date,to_date,cnxn):
    if(is_admin==6 or is_admin==5):
        df=paymentMode_level(office_id,from_date,to_date,-1,cnxn)
        return(paymentMode_Body(df,cnxn))
    elif(is_admin==4):
        df=paymentMode_level(office_id,from_date,to_date,-1,cnxn)
        df=df[~((df["officeType"]!="Company")& (df["masterOfficeId"].str.lower()==office_id.lower()))]
        return(paymentMode_Body(df,cnxn))
    elif(is_admin==1):
        df=paymentMode_level(office_id,from_date,to_date,1,cnxn)
        df=df[df["officeType"]=="Wholesale Pumps"|df["officeType"]=="Retail Pumps"]
        return(paymentMode_Body(df,cnxn))
    elif(is_admin==3):
        df=paymentMode_level(office_id,from_date,to_date,1,cnxn)
        df=df[df["officeType"]=="Wholesale Pumps"]
        return(paymentMode_Body(df,cnxn))
    elif(is_admin==2):
        df=paymentMode_level(office_id,from_date,to_date,1,cnxn)
        df=df[df["officeType"]=="Retail Pumps"]
        return(paymentMode_Body(df,cnxn))
    elif(is_admin==0):
        df=paymentMode_level(office_id,from_date,to_date,0,cnxn)
        return(paymentMode_Body(df,cnxn))
