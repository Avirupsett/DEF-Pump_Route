import pandas as pd
import warnings
warnings.filterwarnings("ignore")
import datetime


def Extracting( Product_Type,cnxn):
    Begindate = datetime.datetime.now()
    Begindate = Begindate.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
   
    df=pd.read_sql_query(f'''
SELECT
    df.OfficeId,
        df.OfficeName,
    df.Longitude,
        df.Latitude,
        df.ProductTypeId,
        df.CurrentStock,
        df.totalCapacity,
        df.avgSales,
        df.masterOfficeId,
        df.masterOfficeName

    FROM(
    SELECT
        o.OfficeId,
        o.masterOfficeId,
        m.OfficeName As masterOfficeName,
        o.OfficeName,
        o.Longitude,
        o.Latitude,
        cs.ProductTypeId,
        cs.CurrentStock,
        gm.totalCapacity,
        s.avgSales
    FROM
        (Select * from Office Where IsActive=1) o
    LEFT JOIN
        Office m ON m.OfficeId = o.masterOfficeId
    LEFT JOIN
        CurrentStockDetails cs ON o.OfficeId = cs.OfficeId
    LEFT JOIN
        (
           Select Sum(su.Capacity) As totalCapacity,su.OfficeId From
(SELECT GM2.OfficeId, GM2.Capacity,GPM.ProductId
        FROM
            GodownMaster GM2
		Left Join
GodownProductMapper GPM ON GPM.GodownId=GM2.GodownId)su
Where su.ProductId={Product_Type}
Group By
su.OfficeId
        ) gm ON o.OfficeId = gm.OfficeId
    LEFT JOIN
        (
            SELECT
                OfficeId,
                AVG(Quantity) AS avgSales
            FROM
                Sales
            WHERE
                Total > 0 AND IsDeleted=0
            GROUP BY
                OfficeId
        ) s ON o.OfficeId = s.OfficeId
        )df
    WHERE
        df.OfficeId NOT IN (
    Select 
    d.OfficeId
    FROM
    (
    SELECT
        dp.DeliveryPlanStatusId,
        dp.DeliveryPlanId,
        dp.PlanDate,
        dp.ExpectedDeliveryDate,
        dp.ProductId,
        dpd.OfficeId,
        dpd.DeliveryPlanDetailsStatusId,
        dpd.ReceivedQuantity
    FROM

        DeliveryPlan dp

        left join
        DeliveryPlanDetails dpd 
        on dp.DeliveryPlanId=dpd.DeliveryPlanId

        WHERE
        dp.ProductId = {Product_Type}
        AND ( dp.DeliveryPlanStatusId <= 6 AND (dpd.ReceivedQuantity=0 OR dpd.ReceivedQuantity=NULL))
        AND ( dpd.DeliveryPlanDetailsStatusId!=3)
        )As d
)  ;
 ''',cnxn)
    df.rename(
            columns={
                "OfficeId": "officeId",
                "OfficeName": "officeName",
                "Longitude": "longitude",
                "Latitude": "latitude",
                "CurrentStock": "currentStock",
                "ProductTypeId": "productTypeId",
            },
            inplace=True,
        )
   

    # if totalCapacity value is 0 then replace it to 2000
    df[["currentStock", "totalCapacity"]].fillna(0, inplace=True)
    df["totalCapacity"].replace(to_replace=0, value=2000, inplace=True)

    df["requirement%"] = (
        abs(df["totalCapacity"] - df["currentStock"]) / df["totalCapacity"]
    ) * 100

    df = df[df["productTypeId"] == Product_Type]
    

    df["requirement%"].fillna(0, inplace=True)
    df["avgSales"].fillna(0, inplace=True)
    df.dropna(inplace=True)
    
    df.sort_values(by="requirement%", inplace=True, ascending=False)
    df.reset_index(inplace=True, drop=True)
    

    return df


def ExtractingFromOfficeId( Product_Type, OfficeList,cnxn,No_of_days_for_delivery,minimum_multiple):

    df=pd.read_sql_query(f'''SELECT
    o.OfficeId,
    o.OfficeName,
    o.Longitude,
    o.Latitude,
    cs.ProductTypeId,
    cs.CurrentStock,
    gm.totalCapacity,
    s.avgSales,
    m.masterOfficeId,
    m.OfficeName As masterOfficeName
FROM
    (Select * from Office Where IsActive=1) o
LEFT JOIN
        Office m ON m.OfficeId = o.masterOfficeId
LEFT JOIN
    CurrentStockDetails cs ON o.OfficeId = cs.OfficeId

LEFT JOIN
    (
        Select Sum(su.Capacity) As totalCapacity,su.OfficeId From
(SELECT GM2.OfficeId, GM2.Capacity,GPM.ProductId
        FROM
            GodownMaster GM2
		Left Join
GodownProductMapper GPM ON GPM.GodownId=GM2.GodownId)su
Where su.ProductId={Product_Type}
Group By
su.OfficeId
    ) gm ON o.OfficeId = gm.OfficeId
LEFT JOIN
    (
        SELECT
            OfficeId,
            AVG(Quantity) AS avgSales
        FROM
            Sales
        WHERE
            Total > 0 AND isDeleted=0
        GROUP BY
            OfficeId
    ) s ON o.OfficeId = s.OfficeId
	Where
o.OfficeId IN {tuple(OfficeList) if len(OfficeList)>1 else f"('{OfficeList[0]}')"}

    ;
''',cnxn)
    df.rename(
            columns={
                "OfficeId": "officeId",
                "OfficeName": "officeName",
                "Longitude": "longitude",
                "Latitude": "latitude",
                "CurrentStock": "currentStock",
                "ProductTypeId": "productTypeId",
            },
            inplace=True,
        )
    # if totalCapacity value is 0 then replace it to 2000
    df[["currentStock", "totalCapacity"]].fillna(0, inplace=True)
    df["totalCapacity"].replace(to_replace=0, value=2000, inplace=True)

    df["requirement%"] = (
        abs(df["totalCapacity"] - df["currentStock"]) / df["totalCapacity"]
    ) * 100

    df = df[df["productTypeId"] == Product_Type]
   

    df["requirement%"].fillna(0, inplace=True)
    df["avgSales"].fillna(0, inplace=True)
    # df.dropna(inplace=True)

    df["atDeliveryRequirement"]=df["totalCapacity"]-df["currentStock"]+df["avgSales"]*No_of_days_for_delivery 
    df.reset_index(inplace=True)
    for i in range(len(df)):
        if df.loc[i,"atDeliveryRequirement"]>df.loc[i,"totalCapacity"]:
            df.loc[i,"atDeliveryRequirement"]=df.loc[i,"totalCapacity"]
        else:
            df.loc[i,"atDeliveryRequirement"]=df.loc[i,"atDeliveryRequirement"]
    # print(df.apply(lambda row: row["totalCapacity"] if row["atDeliveryRequirement"] > row["totalCapacity"] else row["atDeliveryRequirement"],axis=1))
    # df["atDeliveryRequirement"] = df.apply(lambda row: row["totalCapacity"] if row["atDeliveryRequirement"] > row["totalCapacity"] else row["atDeliveryRequirement"])

    df["requirement%"]=df["atDeliveryRequirement"]/df["totalCapacity"]*100
    df["requirement%"].fillna(0,inplace=True)
    df["atDeliveryRequirement"]= (df["atDeliveryRequirement"]//minimum_multiple)*minimum_multiple
    # df["currentStock"]=df["currentStock"]-df["avgSales"]*No_of_days_for_delivery
    df["availableQuantity"]=df["totalCapacity"]-(df["currentStock"]-df["avgSales"]*No_of_days_for_delivery)
    df["atDeliveryRequirement"].replace(to_replace=0, value=minimum_multiple, inplace=True)
    
    df.sort_values(by="requirement%", inplace=True, ascending=False)
    df.reset_index(inplace=True, drop=True)

    total_requirement=df['atDeliveryRequirement'].dropna().sum()

    df2=Extracting( Product_Type,cnxn)
    
    office_list=df["officeId"].to_list()
    Not_selected=df2[~df2["officeId"].isin(office_list)]

    # Not_selected=pd.merge(df2,df,indicator=True,how='outer').query('_merge=="left_only"').drop('_merge',axis=1)
    Not_selected["atDeliveryRequirement"]=Not_selected["totalCapacity"]-Not_selected["currentStock"]+Not_selected["avgSales"]*No_of_days_for_delivery
    Not_selected.reset_index(inplace=True)
    for i in range(len(Not_selected)):
        if Not_selected.loc[i,"atDeliveryRequirement"]>Not_selected.loc[i,"totalCapacity"]:
            Not_selected.loc[i,"atDeliveryRequirement"]=Not_selected.loc[i,"totalCapacity"]
        else:
            Not_selected.loc[i,"atDeliveryRequirement"]=Not_selected.loc[i,"atDeliveryRequirement"]

        # Not_selected["atDeliveryRequirement"] = Not_selected.apply(lambda row: row["totalCapacity"] if row["atDeliveryRequirement"] > row["totalCapacity"] else row["atDeliveryRequirement"])

    Not_selected["requirement%"]=Not_selected["atDeliveryRequirement"]/Not_selected["totalCapacity"]*100
    Not_selected["requirement%"].fillna(0,inplace=True)
    Not_selected["atDeliveryRequirement"]= (Not_selected["atDeliveryRequirement"]//minimum_multiple)*minimum_multiple
    # Not_selected["currentStock"]=Not_selected["currentStock"]-Not_selected["avgSales"]*No_of_days_for_delivery
    Not_selected["availableQuantity"]=Not_selected["totalCapacity"]-(Not_selected["currentStock"]-Not_selected["avgSales"]*No_of_days_for_delivery)
    Not_selected["atDeliveryRequirement"].replace(to_replace=0, value=minimum_multiple, inplace=True)
                                     
    
    Not_selected.sort_values(by="requirement%",inplace=True,ascending=False)
    Not_selected.reset_index(drop=True,inplace=True)

    return df,total_requirement,Not_selected[["officeName","latitude","longitude","atDeliveryRequirement","officeId","totalCapacity","currentStock","availableQuantity","masterOfficeId","masterOfficeName"]]
