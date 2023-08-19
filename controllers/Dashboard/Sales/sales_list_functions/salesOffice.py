import pandas as pd

def total_sales_based_on_office_body(df):
    alldata=[]
    for office in (df[df["level"]==1]["officeId"].unique()):
            if(len(df[df["masterOfficeId"].str.lower()==office.lower()])):
                totalIncome=df[df["masterOfficeId"].str.lower()==office.lower()]["totalIncome"].sum()
                for innerOffice in df[df["masterOfficeId"].str.lower()==office.lower()]["officeId"].unique():
                    totalIncome+=df[df["masterOfficeId"].str.lower()==innerOffice.lower()]["totalIncome"].sum()
                alldata.append({
                    "officeId":office,
                    "officeName":df[df["masterOfficeId"].str.lower()==office.lower()]["masterOfficeName"].unique()[0],
                    "officeTypeColor":df[df["officeId"].str.lower()==office.lower()]["officeTypeColor"].unique()[0],
                    "officeType":df[df["officeId"].str.lower()==office.lower()]["officeType"].unique()[0],
                    "totalIncome":totalIncome
                })
            else:
                totalIncome=df[df["officeId"].str.lower()==office.lower()]["totalIncome"].sum()
                alldata.append({
                    "officeId":office,
                    "officeName":df[df["officeId"].str.lower()==office.lower()]["officeName"].unique()[0],
                    "officeTypeColor":df[df["officeId"].str.lower()==office.lower()]["officeTypeColor"].unique()[0],
                    "officeType":df[df["officeId"].str.lower()==office.lower()]["officeType"].unique()[0],
                    "totalIncome":totalIncome
                })
    final_df=pd.DataFrame(alldata)
    try:
        final_df=(final_df.sort_values(by=["officeType","officeName"],ascending=[False,False],key=lambda x:x.str.lower())).reset_index(drop=True)
    except:
        print("No Data Found")

    return final_df.to_dict('records')