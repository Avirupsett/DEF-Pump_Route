import pandas as pd

def total_sales_based_on_customer_body(df):
    df_by_name=pd.DataFrame()
    df_by_mobile=pd.DataFrame()
    df_by_vehicle=pd.DataFrame()
    
    try:
        df_by_name_mask=df[df["CustomerName"]!=""]
        if not df_by_name_mask.empty:
            df_by_name_mask["CustomerName"]=df_by_name_mask["CustomerName"].str.upper().str.strip()
            df_by_name_mask=df_by_name_mask[df_by_name_mask["CustomerName"]!="XXX"]
            df_by_name=df_by_name_mask.groupby(["CustomerName"],as_index=True).agg(total=("totalIncome","sum"),count=("totalIncome","count"),filterName=("CustomerName","first")).sort_values(by=["total"],ascending=True).reset_index(drop=True)[-10:]
        

        df_by_mobile_mask=df[df["MobileNo"]!=""]
        if not df_by_mobile_mask.empty:
            df_by_mobile_mask["MobileNo"]=df_by_mobile_mask["MobileNo"].str.replace(' ', '')
            df_by_mobile_mask=df_by_mobile_mask[df_by_mobile_mask["MobileNo"]!="0000"]
            df_by_mobile=df_by_mobile_mask.groupby(["MobileNo"],as_index=True).agg(total=("totalIncome","sum"),count=("totalIncome","count"),filterName=("MobileNo","first")).sort_values(by=["total"],ascending=True).reset_index(drop=True)[-10:]


        df_by_vehicle_mask=df[df["VehicleNo"]!=""]
        if not df_by_vehicle_mask.empty:
            df_by_vehicle_mask["VehicleNo"]=df_by_vehicle_mask["VehicleNo"].str.replace(' ', '').str.upper()
            df_by_vehicle_mask=df_by_vehicle_mask[df_by_vehicle_mask["VehicleNo"]!="XXX"]
            df_by_vehicle=df_by_vehicle_mask.groupby(["VehicleNo"],as_index=True).agg(total=("totalIncome","sum"),count=("totalIncome","count"),filterName=("VehicleNo","first")).sort_values(by=["total"],ascending=True).reset_index(drop=True)[-10:]

    except:
        print("Sales Customer Error")
  
        # Group the data by vehicle number and collect mobile numbers as lists
        # grouped = df.groupby('VehicleNo')['MobileNo'].apply(list)

        # # Filter groups where there are multiple mobile numbers
        # vehicles_with_multiple_mobiles = grouped[grouped.apply(len) > 1]

        # for vehicle_number, mobile_numbers in vehicles_with_multiple_mobiles.items():
        #     print(f"Vehicle {vehicle_number} has multiple mobile numbers: {', '.join(mobile_numbers)}")
        


    
    return {"byName":df_by_name.to_dict('records'),"byMobile":df_by_mobile.to_dict('records'),"byVehicle":df_by_vehicle.to_dict('records')}