import pandas as pd

# def total_sales_based_on_office_body(df):
#     alldata=[]
#     for office in (df[df["level"]==1]["officeId"].unique()):
#             if(len(df[df["masterOfficeId"].str.lower()==office.lower()])):
#                 totalIncome=df[df["masterOfficeId"].str.lower()==office.lower()]["totalIncome"].sum()
#                 for innerOffice in df[df["masterOfficeId"].str.lower()==office.lower()]["officeId"].unique():
#                     totalIncome+=df[df["masterOfficeId"].str.lower()==innerOffice.lower()]["totalIncome"].sum()
#                 alldata.append({
#                     "officeId":office,
#                     "officeName":df[df["masterOfficeId"].str.lower()==office.lower()]["masterOfficeName"].unique()[0],
#                     "officeTypeColor":df[df["officeId"].str.lower()==office.lower()]["officeTypeColor"].unique()[0],
#                     "officeType":df[df["officeId"].str.lower()==office.lower()]["officeType"].unique()[0],
#                     "totalIncome":totalIncome
#                 })
#             else:
#                 totalIncome=df[df["officeId"].str.lower()==office.lower()]["totalIncome"].sum()
#                 alldata.append({
#                     "officeId":office,
#                     "officeName":df[df["officeId"].str.lower()==office.lower()]["officeName"].unique()[0],
#                     "officeTypeColor":df[df["officeId"].str.lower()==office.lower()]["officeTypeColor"].unique()[0],
#                     "officeType":df[df["officeId"].str.lower()==office.lower()]["officeType"].unique()[0],
#                     "totalIncome":totalIncome
#                 })
#     final_df=pd.DataFrame(alldata)
#     try:
#         final_df=(final_df.sort_values(by=["officeType","officeName"],ascending=[False,False],key=lambda x:x.str.lower())).reset_index(drop=True)
#     except:
#         print("No Data Found")

#     return final_df.to_dict('records')

def calculate_total_income(master_office_id,df):
    total_income = df[df["officeId"] == master_office_id]["totalIncome"].sum()
    
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
