import pandas as pd
import dataops as ta

# df = pd.read_csv("./files/sample_record.csv")

# df.to_json("./files/sample_record.json", orient='records')

ap1 = ta.ApiTask()

dic_d = dict({"Id":12345,"ActivityDate":"20-08-2022","TotalSteps":12262,"TotalDistance":7.869999886,
         "TrackerDistance":7.869999886,"LoggedActivitiesDistance":0,"VeryActiveDistance":3.319999933,
         "ModeratelyActiveDistance":0.829999983,"LightActiveDistance":3.640000105,
         "SedentaryActiveDistance":0,"VeryActiveMinutes":47,"FairlyActiveMinutes":21,
         "LightlyActiveMinutes":200,"SedentaryMinutes":866,"Calories":1868})

records = ap1.delete_data_mongo(dic_d)

print(records)