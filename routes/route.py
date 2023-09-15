from flask import jsonify, request
import pandas as pd
import numpy as np
import pyodbc
import warnings
import os
from datetime import datetime,timedelta
warnings.filterwarnings("ignore")
from config.config import ConnectionString

from controllers.Extraction.extraction import Extracting, ExtractingFromOfficeId
from controllers.Extraction.extraExtraction import ExtractingFromDeliveryPlan
from controllers.Dashboard.Sales.dropdown_list import dropdown_list
from controllers.Dashboard.Sales.sales_list import sales_based_on_admin
from controllers.Dashboard.Sales.ExistingCurrent import ExistingCurrentCustomer
from controllers.Dashboard.Sales.sales_customer import total_sales_based_on_customer
from controllers.Dashboard.Sales.total_sales import total_sales
from controllers.Dashboard.Sales.card_details import CardDetails
from controllers.Dashboard.Godown_stocks.godown_list import Godown_list
from controllers.Dashboard.Godown_stocks.godownType_list import GodownType
from controllers.Dashboard.User_Details.user import UserDetails
from controllers.Dashboard.Sales.paymentmode import paymentMode
from controllers.Filteration.filtration import Filtering
from controllers.RouteFinding.Algo01 import Route_plan_without_priority
from controllers.Dashboard.Driver.metrics import driver_metrics 

from flask import Blueprint

route_page = Blueprint("simple_page", __name__)

@route_page.route("/api/v1/route_plan", methods=["POST"])
def create_post():
    request_data = request.get_json()

    Product_TypeId = None
    Starting_PointId = None
    Tank_Capacity = None
    minimum_multiple = None
    No_of_days_for_delivery = None
    DeliveryPlanId = None
    Office_list = None
    Starting_PointName = None
    Starting_Point_latitude = None
    Starting_Point_longitude = None
    PlanDateTime=None
    DeliveryDateTime=None
    No_of_days_for_delivery=None
    Stoppage_unloading_time=60
    Extra_unloading_time=30
    speed_of_vehicle=40

    if request_data:
        if "ProductTypeId" in request_data:
            Product_TypeId = request_data["ProductTypeId"]

        if "StartingPointId" in request_data:
            Starting_PointId = request_data["StartingPointId"]

        if "TankCapacity" in request_data:
            Tank_Capacity = request_data["TankCapacity"]

        if "MinimumMultiple" in request_data:
            minimum_multiple = request_data["MinimumMultiple"]

        if "No_of_days_for_delivery" in request_data:
            No_of_days_for_delivery = request_data["No_of_days_for_delivery"]
        
        if "PlanDateTime" in request_data:
            PlanDateTime = request_data["PlanDateTime"]

        if "DeliveryDateTime" in request_data:
            DeliveryDateTime = request_data["DeliveryDateTime"]

        if "DeliveryPlanId" in request_data:
            DeliveryPlanId = request_data["DeliveryPlanId"]

        if "OfficeIdList" in request_data:
            Office_list = request_data["OfficeIdList"]

        if "SpeedOfVehicle" in request_data:
            if request_data["SpeedOfVehicle"]:
                speed_of_vehicle = request_data["SpeedOfVehicle"]

        if "StoppageUnloadingTime" in request_data:
            if request_data["StoppageUnloadingTime"]:
                Stoppage_unloading_time = request_data["StoppageUnloadingTime"]

        if "ExtraUnloadingTime" in request_data:
            if request_data["ExtraUnloadingTime"]:
                Extra_unloading_time = request_data["ExtraUnloadingTime"]

    cnxn = pyodbc.connect(ConnectionString)
    date_format = "%Y-%m-%d %H:%M:%S"
    if PlanDateTime is not None and DeliveryDateTime is not None:
        start_datetime = datetime.strptime(PlanDateTime, date_format).date()
        end_datetime = datetime.strptime(DeliveryDateTime, date_format).date()

        time_difference = end_datetime - start_datetime
        No_of_days_for_delivery = time_difference.days
    # else:
    #     No_of_days_for_delivery = 0

    if DeliveryPlanId:
        (
            df,
            Starting_PointId,
            Starting_PointName,
            Starting_Point_latitude,
            Starting_Point_longitude,
            total_requirement,
            excess_capacity,
            Not_selected,
            DeliveryLimit,PlanDate,ExpectedDeliveryDate,DeliveryPlanStatusId,CreatedBy,UpdatedBy,CreatedOn,UpdatedOn
        ) = ExtractingFromDeliveryPlan(DeliveryPlanId, cnxn,No_of_days_for_delivery)
        optimal_route1 = Route_plan_without_priority(
        df,
        Starting_PointName,
        str(Starting_PointId),
        Starting_Point_latitude,
        Starting_Point_longitude,
        ExpectedDeliveryDate,
        speed_of_vehicle
    )
        if (len(df)>0):
            df["DeliveredAt"]=pd.to_datetime(df["DeliveredAt"]).dt.strftime('%Y-%m-%d %H:%M:%S')
            df=pd.merge(optimal_route1[0],df[["officeId","AdminId","DeliveryPlanId","DeliveryPlanDetailsId","SequenceNo","ReceivedQuantity","ApprovedQuantity","DeliveryPlanStatusId","ApproveStatus","DeliveredQuantity","DeliveredAt"]],on="officeId",how="left")
            df = df.replace({np.nan: None})
            time=0
            for i in range(len(df)):
     
                if  i>1:
                    time+=int((df.loc[i-1,"ApprovedQuantity"]-1000)//500)*Extra_unloading_time+Stoppage_unloading_time if df.loc[i-1,"ApprovedQuantity"] and int(df.loc[i-1,"ApprovedQuantity"])>1000 else Stoppage_unloading_time
                    addedTime=datetime.strptime(df.loc[i,"estimatedDeliveryTime"], date_format) + timedelta(minutes=time)
                    df.loc[i,"estimatedDeliveryTime"]=datetime.strftime(addedTime,date_format)
            

        return jsonify(
        Plan_date=PlanDate,
        ExpectedDelivery_date=ExpectedDeliveryDate,
        DeliveryPlan_statusId=DeliveryPlanStatusId,
        Created_by=CreatedBy,
        Updated_by=UpdatedBy,
        Created_on=CreatedOn,
        Updated_on=UpdatedOn,
        Delivery_limit=DeliveryLimit,
        StartPoint_id=int(Starting_PointId),
        Total_requirement=total_requirement,
        Excess_capacity=excess_capacity,
        Not_selected=Not_selected,
        Routes={
            "Algorithm_1": {
                "Description": "Routing based on Nearest Branch",
                "Route": df.to_dict(orient="records"),
                "Total_distance": optimal_route1[1],
            },
        },
    )

    elif Office_list and len(Office_list) > 0:
        df,total_requirement,Not_selected2 = ExtractingFromOfficeId(
            Product_TypeId, Office_list, cnxn, No_of_days_for_delivery, minimum_multiple
        )

        excess_capacity=Tank_Capacity-total_requirement if Tank_Capacity is not None else None
        Not_selected = Not_selected2.to_dict(orient="records")

        Starting_Point_df = pd.read_sql_query(
            f"Select HubName,Latitude,Longitude from Hub where HubId={Starting_PointId}",
            cnxn,
        )
        Starting_PointName = Starting_Point_df["HubName"][0]
        Starting_Point_latitude = Starting_Point_df["Latitude"][0]
        Starting_Point_longitude = Starting_Point_df["Longitude"][0]
    else:
        df = Extracting(Product_TypeId, cnxn)

        df, total_requirement, excess_capacity, Not_selected = Filtering(
            df, Tank_Capacity, No_of_days_for_delivery, minimum_multiple
        )
        Not_selected = Not_selected.to_dict(orient="records")
        Starting_Point_df = pd.read_sql_query(
            f"Select HubName,Latitude,Longitude from Hub where HubId={Starting_PointId}",
            cnxn,
        )
        Starting_PointName = Starting_Point_df["HubName"][0]
        Starting_Point_latitude = Starting_Point_df["Latitude"][0]
        Starting_Point_longitude = Starting_Point_df["Longitude"][0]

    cnxn.close()

    optimal_route1 = Route_plan_without_priority(
        df,
        Starting_PointName,
        str(Starting_PointId),
        Starting_Point_latitude,
        Starting_Point_longitude,
        DeliveryDateTime,
        speed_of_vehicle
    )
    optimal_route1_df=optimal_route1[0]
    time=0
    for i in range(len(optimal_route1_df)):
        if i>1:
            time+=int((optimal_route1_df.loc[i-1,"atDeliveryRequirement"]-1000)//500)*Extra_unloading_time+Stoppage_unloading_time if optimal_route1_df.loc[i-1,"atDeliveryRequirement"] and int(optimal_route1_df.loc[i-1,"atDeliveryRequirement"])>1000 else Stoppage_unloading_time
            addedTime=datetime.strptime(optimal_route1_df.loc[i,"estimatedDeliveryTime"], date_format) + timedelta(minutes=time)
            optimal_route1_df.loc[i,"estimatedDeliveryTime"]=datetime.strftime(addedTime,date_format)

    return jsonify(
        Total_requirement=total_requirement,
        Excess_capacity=excess_capacity,
        Not_selected=Not_selected,
        Routes={
            "Algorithm_1": {
                "Description": "Routing based on Nearest Branch",
                "Route": optimal_route1_df.to_dict(orient="records"),
                "Total_distance": optimal_route1[1],
            },
        },
    )


@route_page.route("/api/v1/dashboard/dropdown_list/<string:_UserId>", methods=["GET"])
def dropdown(_UserId):
    user_id = _UserId
  
    cnxn = pyodbc.connect(ConnectionString)
    df = dropdown_list(user_id,cnxn)
    cnxn.close()
    return jsonify(df.to_dict(orient="records"))


@route_page.route("/api/v1/dashboard/sales_list/<string:_FromDate>/<string:_ToDate>/<string:_OfficeId>/<string:_IsAdmin>", methods=["GET"])
def sales_list(_FromDate,_ToDate,_OfficeId,_IsAdmin):
    from_date = _FromDate
    to_date = _ToDate
    office_id = _OfficeId
    is_admin = int(_IsAdmin)

    cnxn = pyodbc.connect(ConnectionString)
    Sales_Expense_df,product_type_list,sales_based_on_office,sales_based_on_customer,paymentMode = sales_based_on_admin(office_id,is_admin,from_date,to_date,cnxn)
    cnxn.close()
    return {"graph1":Sales_Expense_df,"graph2":product_type_list,"graph3":sales_based_on_office,"graph4":sales_based_on_customer,"graph5":paymentMode}

@route_page.route("/api/v1/dashboard/customer_list/<string:_FromDate>/<string:_ToDate>/<string:_OfficeId>/<string:_IsAdmin>", methods=["GET"])
def Customer_list(_FromDate,_ToDate,_OfficeId,_IsAdmin):
    from_date = _FromDate
    to_date = _ToDate
    office_id = _OfficeId
    is_admin = int(_IsAdmin)

    cnxn = pyodbc.connect(ConnectionString)
    df = ExistingCurrentCustomer(office_id,is_admin,from_date,to_date,cnxn)
    cnxn.close()
    return jsonify(df)

@route_page.route("/api/v1/dashboard/sales_customer/<string:_FromDate>/<string:_ToDate>/<string:_OfficeId>/<string:_IsAdmin>", methods=["GET","POST"])
def sales_customer(_FromDate,_ToDate,_OfficeId,_IsAdmin):
    from_date = _FromDate
    to_date = _ToDate
    office_id = _OfficeId
    is_admin = int(_IsAdmin)
    CustomerName = None
    MobileNo = None
    VehicleNo = None

    request_data = request.get_json()
    if request_data:
        if "CustomerName" in request_data:
            CustomerName=request_data["CustomerName"]
        if "MobileNo" in request_data:
            MobileNo=request_data["MobileNo"]
        if "VehicleNo" in request_data:
            VehicleNo=request_data["VehicleNo"]

    cnxn = pyodbc.connect(ConnectionString)
    df = total_sales_based_on_customer(office_id,is_admin,from_date,to_date,cnxn,CustomerName,MobileNo,VehicleNo)
    cnxn.close()
    return jsonify(df)

@route_page.route("/api/v1/dashboard/total_sales/<string:_FromDate>/<string:_ToDate>/<string:_OfficeId>/<string:_IsAdmin>", methods=["GET"])
def total_sales_list(_FromDate,_ToDate,_OfficeId,_IsAdmin):
    from_date = _FromDate
    to_date = _ToDate
    office_id = _OfficeId
    is_admin = int(_IsAdmin)

    cnxn = pyodbc.connect(ConnectionString)
    df=total_sales(office_id,is_admin,from_date,to_date,cnxn)
    cnxn.close()
    return jsonify({"graph1":df})

@route_page.route("/api/v1/dashboard/card_details_list/<string:_OfficeId>/<string:_IsAdmin>", methods=["GET"])
def card_details_list(_OfficeId,_IsAdmin):

    office_id = _OfficeId
    is_admin = int(_IsAdmin)

    cnxn = pyodbc.connect(ConnectionString)
    json = CardDetails(office_id,is_admin,cnxn)
    cnxn.close()
    return json

@route_page.route("/api/v1/dashboard/godown_details_list/<string:_OfficeId>/<string:_IsAdmin>", methods=["GET"])
def godown_details_list(_OfficeId,_IsAdmin):
    
    office_id = _OfficeId
    is_admin = int(_IsAdmin)

    cnxn = pyodbc.connect(ConnectionString)
    json = Godown_list(office_id,is_admin,cnxn)
    cnxn.close()
    return jsonify(json)

@route_page.route("/api/v1/dashboard/payment/<string:_FromDate>/<string:_ToDate>/<string:_OfficeId>/<string:_IsAdmin>", methods=["GET"])
def payment_details(_FromDate,_ToDate,_OfficeId,_IsAdmin):

    from_date = _FromDate
    to_date = _ToDate
    office_id = _OfficeId
    is_admin = int(_IsAdmin)

    cnxn = pyodbc.connect(ConnectionString)
    json = paymentMode(office_id,is_admin,from_date,to_date,cnxn)
    cnxn.close()
    return json

@route_page.route("/api/v1/dashboard/godowntype/<string:_OfficeId>/<string:_IsAdmin>", methods=["GET"])
def godownType(_OfficeId,_IsAdmin):

    office_id = _OfficeId
    is_admin = int(_IsAdmin)

    cnxn = pyodbc.connect(ConnectionString)
    json = GodownType(office_id,is_admin,cnxn)
    cnxn.close()
    return json

@route_page.route("/api/v1/dashboard/userdetails/<string:_UserId>", methods=["GET"])
def Userdetails(_UserId):

    user_id = _UserId

    cnxn = pyodbc.connect(ConnectionString)
    json = UserDetails(user_id,cnxn)
    cnxn.close()
    return jsonify(json)

@route_page.route("/api/v1/dashboard/driver/metrics/<string:_DriverId>", methods=["GET"])
def DriverMetrics(_DriverId):

    driver_id = _DriverId

    cnxn = pyodbc.connect(ConnectionString)
    json = driver_metrics(driver_id,cnxn)
    cnxn.close()
    return jsonify(json)

@route_page.route('/api/uploader', methods = ['POST'])
def upload_file():
   if request.method == 'POST':
      f = request.files['file']
      f.save(f"{os.getcwd()}/static/downloads/{f.filename}")
      return {"url":f.filename}