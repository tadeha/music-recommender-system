import pandas as pd

df=pd.read_csv('export.csv')


main=pd.read_csv('result/trackCollectionContentBased.csv')


dic={}
for i in main.columns:
    dic[i]=0.0


for i,line in df.iterrows():
    d=dict(dic)
    t_id=line.TRACK_ID
    tags=line.TAG_IDS[2:-2].replace('"','').split(',')
    types=line.TYPE_KEYS[2:-2].replace('"','').split(',')
    print(t_id,tags,types)
    d['TRACK_ID']=int(t_id)
    for tag in tags:
        d['TAG_ID_' + str(tag)] = 1.0
    for typ in types:
        d['TYPE_KEY_' + str(typ)] = 1.0
main.loc[len(main)] = list(d.values())

main.to_csv('../results/trackCollectionContentBased.csv')
