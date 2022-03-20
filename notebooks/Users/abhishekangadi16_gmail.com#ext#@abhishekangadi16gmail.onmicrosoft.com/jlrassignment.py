# Databricks notebook source
###Importing required librarires
import pandas as pd
import numpy as np

# COMMAND ----------

root_path = "/mnt/jlrassig1"
def importing(filename):
    """Takes in the filename and returns the dataframe"""
    
#     root_path = r"C:\Users\dellpc\Desktop\JLR\REQ 72523 - INTERVIEW PREPARTION INFORMATION"
    filepath = root_path + "/"+filename
    return spark.read.format("csv").option("header","true").option("inferSchema","true").load(filepath).toPandas()

# COMMAND ----------

def dfjoiner(df1, df2,leftCol, rightCol, jointype):
    """it is df joiner function which takes the dfs as input along with the joining keys and return the joined df"""
    
    #Join df1 on df2 over the columns specified
    return pd.merge(df1,df2, left_on =[leftCol], right_on=[rightCol],how = jointype )

def transformations(df_joined, df_options):
    """it is the transformation function which is used to apply all the transformations"""
    
    #condition 1: if sales value is 0 or negative then production_cost should be 0
    #condition 2: if there is exact match then material_cost is production_cost
    df_joined['Production_cost'] = df_joined[['Sales_Price', 'Material_Cost']].apply(lambda x:x['Material_Cost'] if x['Sales_Price']> 0 else 0, axis = 1) 
    
    
    #consditions 3, 4
    #3: if there is no exact match in the options dataset, then use the average across all models for that option code
    #4: if there is no matching options code then assume that the production_cost is 0.45 of the sales price
    
    for group, group_df in df_joined[df_joined['Material_Cost'].isna()].groupby(['Options_Code']):
        searched_df = df_options[(df_options['Option_Code']== group)]       
        if len(searched_df) == 0:
            df_joined.loc[group_df.index,'Production_cost'] = (df_joined.loc[group_df.index, 'Sales_Price']*0.45)
        else:
            df_joined.loc[group_df.index,'Production_cost'] = searched_df['Material_Cost'].mean()
                                                                                     
                                                                                 
        
    return df_joined
            


# COMMAND ----------

###calling the above functions

#creating the dataframe

df_base = importing('base_data.csv')
df_options = importing('options_data.csv')


#Data Preparation

df_base['Model'] = df_base['Model_Text'].str[:4]

#making composite key for the purpose of joining
df_base['keybase'] = df_base['Model']+df_base['Options_Code']
df_options['keyOptions'] = df_options['Model']+df_options['Option_Code']
df_options2 = df_options[['keyOptions','Material_Cost', 'Option_Code', 'Model']]


# COMMAND ----------

###Data Validation

#to check and handle for the schema error
try:
    if df_base["Sales_Price"].dtype == 'float64':
        pass
    else:
        df_base["Sales_Price"] = pd.to_numeric(df_base["Sales_Price"], errors='coerce')
except:
    print('Please check the schema of base_data')
    
    
try:
    if df_options['Material_Cost'].dtype == 'float64':
        pass
    else:
        df_options['Material_Cost'] = pd.to_numeric(df_options['Material_Cost'], errors='coerce')
except:
    print('Please check the schema of options_data')
    
    
# to check and handle the NaN's   
if len(df_base[df_base['VIN'].isna()]) > 0 :
    df_base = df_base[df_base['VIN'].notnull()]
if len(df_options[df_options['Option_Code'].isna()]) > 0 :
    df_options = df_options[df_options['Option_Code'].notnull()]

# COMMAND ----------

#joining the two df

df_join = dfjoiner(df_base, df_options2,'keybase','keyOptions','left')

#calling the transformation function which applies conditions 1,3,4

df_transformed = transformations(df_join, df_options2)
df_transformed['Production_cost'] = df_transformed[['Sales_Price', 'Production_cost']].apply(lambda x:x['Production_cost'] if x['Sales_Price']> 0 else 0, axis = 1) 

#calculating the profit
df_transformed['Profit'] = df_transformed['Sales_Price']-df_transformed['Production_cost']


# COMMAND ----------

#joining the vehicle data with the transformed data
df_vehMap = importing('vehicle_line_mapping.csv')
df_main = dfjoiner(df_transformed, df_vehMap,'Model_x','nameplate_code','left')
df_Profit = df_main.drop(['Model_x','keybase','keyOptions','Option_Code','Model_y', 'nameplate_code'], axis = 1)

# COMMAND ----------

#Loading the file from ADLS to check if the data is new or the same.
path_output = list(dbutils.fs.ls("dbfs:/mnt/jlrassig1/outputds/"))[-1].path
df_read_check = spark.read.format("csv").option("header","true").option("inferSchema","true").load(path_output).toPandas()


df_write = pd.merge(df_Profit, df_read_check, how='outer', indicator='Exist')
df_write = df_write.loc[df_write['Exist'] != 'both']



# COMMAND ----------

#Appending the dataframe in existing one
if len(df_write) == 0:
    pass
else:
    ProfitDF=spark.createDataFrame(df_write)
    ProfitDF.coalesce(1).write.mode("append").format("csv").option("header","true").csv("/mnt/jlrassig1/outputds")

# COMMAND ----------

#Grouping the dataframe per options per veicle with aggegrate function as sum.

df_grouped_OC = df_Profit.groupby(['Options_Code','Model_Text'])['Profit'].agg(['sum','count']).reset_index()
df_grouped_OC_s = df_grouped_OC.sort_values('sum', ascending = False)
df_grouped_OC_s['Model'] = df_grouped_OC_s['Options_Code']+'-'+df_grouped_OC_s['Model_Text'].str[:4]



import matplotlib.pyplot as plt
x = df_grouped_OC_s['Model'][:20]
y = df_grouped_OC_s['sum'][:20]

plt.bar(x,y)
plt.xticks(rotation=60)
plt.xlabel('OPtions_Code')
plt.ylabel('Total Profit £')
plt.title('Profit Per Top 20 Options')
plt.show()

# COMMAND ----------

df_grouped_OC = df_Profit.groupby(['Options_Code','Model_Text'])['Profit'].agg(['sum','count']).reset_index()
df_grouped_OC_s = df_grouped_OC.sort_values('sum', ascending = False)
# import matplotlib.pyplot as plt
df_grouped_OC_s['Options_Code']+'-'+df_grouped_OC_s['Model_Text'].str[:4]

# COMMAND ----------

df_grouped_OC_s['Options_Code']+'-'+df_grouped_OC_s['Model_Text'].str[:4]

# COMMAND ----------

### Analytics over Profit table
#max profit value

# df_Profit[df_Profit['Profit'] == df_Profit['Profit'].max()]

# COMMAND ----------

#max profit value

# df_Profit[df_Profit['Profit'] == df_Profit['Profit'].min()]

# COMMAND ----------

df_option_p = df_Profit[df_Profit['Model_Text'].str[:4] == 'L405']
df_grouped_OC = df_option_p.groupby(['Options_Code'])['Profit'].agg(['sum','count']).reset_index()
df_bar = df_grouped_OC.sort_values('sum', ascending = False)
import matplotlib.pyplot as plt
df_bar1 = df_bar[:20]
x = df_bar1['Options_Code']
y = df_bar1['sum']

plt.bar(x,y)
plt.xticks(rotation=60)
plt.xlabel('Options_Code')
plt.ylabel('Total Profit £')
plt.title('Profit Per bottom 20 options')
plt.show()

# COMMAND ----------

# df_Profit = df_Profit[df_Profit['Model_Text'] == 'L494']
df_grouped_OC = df_Profit.groupby(['Options_Code'])['Profit'].agg(['sum','count']).reset_index()
df_bar = df_grouped_OC.sort_values('sum', ascending = False)
import matplotlib.pyplot as plt
df_bar2 = df_bar[:20]
x = df_bar2['Options_Code']
y = df_bar2['sum']

plt.bar(x,y)
plt.xticks(rotation=60)
plt.xlabel('OPtions_Code')
plt.ylabel('Total Profit £')
plt.title('Profit Per Top 20 Options')
plt.show()

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt
df_grouped_OC = df_Profit.groupby(['Options_Code','Model_Text','Sales_Price','Production_cost'])['Profit'].agg(['sum','count']).reset_index()
df_profit_desc = df_grouped_OC.sort_values('sum', ascending = False)


x = df_profit_desc['count']
y = df_profit_desc['sum']
# colors = np.random.rand(N)
# area = (30 * np.random.rand(N))**2  # 0 to 15 point radii

plt.scatter(x, y)
plt.xlabel('Count[Options+Models]')
plt.ylabel('Profit £')
plt.title('Total_Profit vs Count[Options+Models]')
plt.show()

# COMMAND ----------

df_grouped_OC = df_Profit.groupby(['Options_Code','Option_Desc','Model_Text','Sales_Price','Production_cost'])['Profit'].agg(['sum','count']).reset_index()
df_profit_desc = df_grouped_OC.sort_values('sum', ascending = False)
df_profit_desc

# COMMAND ----------

df_grouped_OC = df_Profit.groupby(['Options_Code','Option_Desc','Model_Text','Sales_Price','Production_cost'])['Profit'].agg(['sum','count']).reset_index()
df = df_grouped_OC.sort_values('sum', ascending = True)

# COMMAND ----------

# df_transformed[(df_transformed['Sales_Price'] < 1) ]['Production_cost'].sum()

# COMMAND ----------

