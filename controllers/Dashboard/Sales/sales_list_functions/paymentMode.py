import pandas as pd

def paymentMode_Body(df,cnxn):
    try:
        if not df.empty:
            payment_df=df.groupby(["PaymentModeName"]).agg(Count=("PaymentModeName","count")).reset_index()
            payment_list=payment_df.to_dict(orient='records')
            payment_mode_names = [item["PaymentModeName"] for item in payment_list]
        else:
            payment_df=pd.DataFrame()
            payment_list=[]
            payment_mode_names=[]
        df2=pd.read_sql_query(f'''Select * From PaymentModeMaster;''',cnxn)
        new_list=df2.to_dict(orient='records')

        for item in new_list:
            if item["PaymentModeName"] not in payment_mode_names:
                payment_list.append({"PaymentModeName":item["PaymentModeName"],"Count":0})
    except:
        print("PaymentMode Error")

    return payment_list