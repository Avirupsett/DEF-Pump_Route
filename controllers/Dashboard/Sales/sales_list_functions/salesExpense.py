import pandas as pd
# import time

def sales_based_on_admin_body(date_range,df1,df2):
    alldata=[]
    productdata=[]
    # start_time=time.time()
    try:
            
        if not df1.empty:
            grouped = df1.groupby(["incomeDate", "productId"]).agg({"totalIncome": "sum","Quantity":"sum","productName":"first","unitName":"first","unitShortName":"first","singularShortName":"first","color":"first"}).reset_index()
            grouped.rename({"totalIncome":"totalSales","Quantity":"qty"},axis=1,inplace=True)
            result = grouped.groupby("incomeDate").apply(lambda group: {
            "date": group["incomeDate"].iloc[0].strftime("%Y-%m-%d"),
            "lstproduct": group[["productId", "totalSales","qty","productName","unitName","unitShortName","singularShortName","color"]].to_dict(orient="records")})
        else:
            result=[]

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
            if pd.to_datetime(i).strftime("%Y-%m-%d") in result:
                productdata.append({
                    "requestedDate": pd.to_datetime(i).strftime("%Y-%m-%d"),
                    "lstproduct": result[pd.to_datetime(i).strftime("%Y-%m-%d")]["lstproduct"]               
                })
            else:
                productdata.append({
                    "requestedDate": pd.to_datetime(i).strftime("%Y-%m-%d"),
                    "lstproduct": []              
                })
    except:
        print("Sales Expense Error")

    # print("Graph1 and Graph2: ",time.time()-start_time)
    
    return alldata,productdata