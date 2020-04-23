import pandas as pd
from sklearn.metrics.pairwise import linear_kernel 
import scipy
import csv
df=pd.read_csv('trackCollectionContentBased.csv')
df.drop(columns=['Unnamed: 0'],inplace=True)
df.drop(columns=['TIME_CREATED','PRICE','PUBLISH_DATE','ALBUM_ID','duration'],inplace=True)
l=list(df.columns)[1:]
ds=df[l]
ds1=ds[ds.index<40000]
ds2=ds.loc[(ds.index >= 40000) & (ds.index <80000)]
ds3=ds[ds.index>=80000]
ds3

temp=scipy.sparse.csr_matrix(ds1.values)
cosine_similarities11 = linear_kernel(temp, temp)
cosine_similarities12 = linear_kernel(temp, scipy.sparse.csr_matrix(ds2.values))
cosine_similarities13 = linear_kernel(temp, scipy.sparse.csr_matrix(ds3.values))

cosine_similarities21 = linear_kernel(scipy.sparse.csr_matrix(ds2.values), scipy.sparse.csr_matrix(ds1.values))
cosine_similarities22 = linear_kernel(scipy.sparse.csr_matrix(ds2.values), scipy.sparse.csr_matrix(ds2.values))
cosine_similarities23 = linear_kernel(scipy.sparse.csr_matrix(ds2.values), scipy.sparse.csr_matrix(ds3.values))

cosine_similarities31 = linear_kernel(scipy.sparse.csr_matrix(ds3.values), scipy.sparse.csr_matrix(ds1.values))
cosine_similarities32 = linear_kernel(scipy.sparse.csr_matrix(ds3.values), scipy.sparse.csr_matrix(ds2.values))
cosine_similarities33 = linear_kernel(scipy.sparse.csr_matrix(ds3.values), scipy.sparse.csr_matrix(ds3.values))

df1=df[df.index<40000]
df2=df.loc[(df.index >= 40000) & (df.index <80000)]
df3=df[df.index>=80000]

results = {}

dff=df1
cosine_sim=cosine_similarities11
for idx, row in dff.iterrows():
	similar_indices = cosine_sim[idx].argsort()[:-20:-1] 
	similar_items = [(cosine_sim[idx][i], dff['TRACK_ID'][i]) for i in similar_indices]
	results[row['TRACK_ID']] = similar_items


dff=df1
cosine_sim=cosine_similarities12
for idx, row in dff.iterrows():
	similar_indices = cosine_sim[idx].argsort()[:-20:-1] 
	similar_items = [(cosine_sim[idx][i], df['TRACK_ID'][i+40000]) for i in similar_indices if cosine_sim[idx][i]>min(results[row['TRACK_ID']])[0]] 
	results[row['TRACK_ID']].extend(similar_items)


ndff=df1
cosine_sim=cosine_similarities13
for idx, row in dff.iterrows():
	similar_indices = cosine_sim[idx].argsort()[:-20:-1] 
	similar_items = [(cosine_sim[idx][i], df['TRACK_ID'][i+80000]) for i in similar_indices if cosine_sim[idx][i]>min(results[row['TRACK_ID']])[0]] 
	results[row['TRACK_ID']].extend(similar_items)

del(cosine_similarities11)
del(cosine_similarities12)
del(cosine_similarities13)

dff=df2
cosine_sim=cosine_similarities21
for idx, row in dff.iterrows():
	similar_indices = cosine_sim[idx-40000].argsort()[:-20:-1] 
	similar_items = [(cosine_sim[idx-40000][i], df['TRACK_ID'][i]) for i in similar_indices] 
	results[row['TRACK_ID']] = similar_items


dff=df2
cosine_sim=cosine_similarities22
for idx, row in dff.iterrows():
	similar_indices = cosine_sim[idx-40000].argsort()[:-20:-1] 
	similar_items = [(cosine_sim[idx-40000][i], df['TRACK_ID'][i+40000]) for i in similar_indices if cosine_sim[idx-40000][i]>min(results[row['TRACK_ID']])[0]] 
	results[row['TRACK_ID']].extend(similar_items)

dff=df2
cosine_sim=cosine_similarities23
for idx, row in dff.iterrows():
	similar_indices = cosine_sim[idx-40000].argsort()[:-20:-1] 
	similar_items = [(cosine_sim[idx-40000][i], df['TRACK_ID'][i+80000]) for i in similar_indices if cosine_sim[idx-40000][i]>min(results[row['TRACK_ID']])[0]] 
	results[row['TRACK_ID']].extend(similar_items)

del(cosine_similarities21)
del(cosine_similarities22)
del(cosine_similarities23)

dff=df3
cosine_sim=cosine_similarities31
for idx, row in dff.iterrows():
	similar_indices = cosine_sim[idx-80000].argsort()[:-20:-1] 
	similar_items = [(cosine_sim[idx-80000][i], df['TRACK_ID'][i]) for i in similar_indices] 
	results[row['TRACK_ID']] = similar_items

dff=df3
cosine_sim=cosine_similarities32
for idx, row in dff.iterrows():
	similar_indices = cosine_sim[idx-80000].argsort()[:-20:-1] 
	similar_items = [(cosine_sim[idx-80000][i], df['TRACK_ID'][i+40000]) for i in similar_indices if cosine_sim[idx-80000][i]>min(results[row['TRACK_ID']])[0]] 
	results[row['TRACK_ID']].extend(similar_items)

dff=df3
cosine_sim=cosine_similarities33
for idx, row in dff.iterrows():
	similar_indices = cosine_sim[idx-80000].argsort()[:-20:-1] 
	similar_items = [(cosine_sim[idx-80000][i], df['TRACK_ID'][i+80000]) for i in similar_indices if cosine_sim[idx-80000][i]>min(results[row['TRACK_ID']])[0]] 
	results[row['TRACK_ID']].extend(similar_items)

del(cosine_similarities31)
del(cosine_similarities32)
del(cosine_similarities33)


del(df1)
del(df2)
del(df3)

del(ds1)
del(ds2)
del(ds3)


for k in results.keys():
    results[k]=sorted(results[k],reverse=True)[:20]


# saving the results

with open('results.csv', 'w', newline="") as csv_file:  
    writer = csv.writer(csv_file)
    for key, value in results.items():
       writer.writerow([key, value])

# load form file
with open('results.csv') as csv_file:
    reader = csv.reader(csv_file)
    mydict = dict(reader)
