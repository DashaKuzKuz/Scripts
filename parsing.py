import pandas as pd
import cx_Oracle
import os
import datetime as dt
import re
from time import sleep
from sqlalchemy import types, create_engine
import shutil
from xlrd import biffh
import sys
import numpy as np



def insert_to_report(expt_txt):
    file = rf'FILES/REPORT.xlsx'
    xl = pd.ExcelFile(file)
    print(xl.sheet_names)
    df1 = xl.parse('REPORT')
    print(df1)
    df2 = pd.DataFrame({"DT_PARSING": [dt.datetime.now()],
                        "TABLE_NM": [lst[i]],
                        "COMMENT": [expt_txt]})
    df1 = df1.append(df2, ignore_index=True, sort=True)
    df1.drop(df1.filter(regex="Unname"), axis=1, inplace=True)
    print(df1)
    with pd.ExcelWriter(rf'FILES/REPORT.xlsx') as writer:
        df1.to_excel(writer, sheet_name='REPORT')
    df1.drop(df1.columns[df1.columns.str.contains('unnamed', case=False)], axis=1, inplace=True)
    writer.save()

if len(sys.argv)>1:
    tns_str = sys.argv[1]
    if len(sys.argv)==7:
        dirs = sys.argv[2:]
        dir,dir_ins,dir_brk,dir_nl,dir_cats = (*dirs,)
    else:
        dir,dir_ins,dir_brk,dir_nl,dir_cats,dir_enc = rf'FILES',rf'FILES',rf'FILES',rf'FILES',rf'FILES',rf'FILES'
else:
    #connecting to sftp
    #tns_str = f''
    dir,dir_ins,dir_brk,dir_nl,dir_cats,dir_enc = rf'FILES',rf'FILES',rf'FILES',rf'FILES',rf'FILES',rf'FILES'

# Языковая настройка
os.environ["NLS_LANG"] = "RUSSIAN_RUSSIA.AL32UTF8"

def date_prep(dfs,colnum):
    for x in range(len(dfs)):
        res = str(dfs[x][colnum]).split('.')[0]
        if len(res) == 8:
            dfs[x][colnum] = dt.datetime.strptime(res, '%H:%M:%S')
        else:
            dfs[x][colnum] = dt.datetime.strptime(res, '%Y-%m-%d %H:%M:%S')

def round_numbers(dfs,column):
    for x in range(len(dfs)):
        dfs[x][column] = round(dfs[x][column],4)

def look_for(dir):
    a = list()

    for (dirpath, dirnames, filenames) in os.walk(dir):
        a += [os.path.join(dirpath, file) for file in filenames]
        
    b = list()
    for (dirpath, dirnames, filenames) in os.walk(dir):
        b += [file for file in filenames]

    files = list(filter(lambda x: x.endswith(('.xls','.xlsx','.xlsm')), a))
    fl_nm = list(filter(lambda x: x.endswith(('.xls','.xlsx','.xlsm')), b))
    if 'flst' not in globals():
        global flst
        flst = []
        flst += fl_nm
    else:
        flst += fl_nm
    files = [file for file in files]
    if 'f_list' not in globals():
        global f_list
        f_list = []
        f_list = [[dir,file] for file in files]
    else:
        f_list += [[dir, file] for file in files]
    if not 'lst' in globals():
        global lst
        lst = []
        lst += files
    else:
        lst += files
    return lst,flst

look_for(dir)

i = 0
k = 0
while i < len(lst):
    print(lst[i])
    type_of_error = None
    try:
        s_names = pd.ExcelFile(lst[i]).sheet_names
    
    #except Exception as e:
    #    insert_to_report(str(e))
    except BaseException as e:
        print(str(e))
        insert_to_report(str(e))
        
    #cheeting
    try:
      print(s_names)
    except (NameError):
            shutil.move(os.path.join(os.path.dirname(lst[i]),flst[i]),os.path.join(dir_brk, flst[i]))
            i += 1
            k = 0
            continue
        
    if 'AMOUNT' in s_names:
        try:
            type_of_error = "Error ONE"
            is_already_exist = 0
            mng_name = str(pd.read_excel(lst[i], sheet_name='information',usecols = 'F' ,skiprows = 2, nrows = 1).values[0][0])
            ABC = pd.read_excel(lst[i], sheet_name='information',usecols = 'D' ,skiprows = 2, nrows = 1).fillna(mng_name).values[0][0]
            fldr_nm = lst[i]
            fldr_nm = fldr_nm[0:fldr_nm.rfind('/')]
            DEF = ''
            if fldr_nm == 'FILES/a' or fldr_nm == 'FILES/c':
                DEF = 'ABCDEFG'
            else:
                DEF = 'HJKLNM'

            type_of_error = "Error TWO"
            df = pd.read_excel(lst[i], sheet_name='AMOUNT',usecols = 'Z:AC' ,skiprows = 143, nrows = 42)
            df.dropna(subset=[df.columns[0]], inplace=True)
            a = pd.melt(df,id_vars=df.columns[0], VEvars=df.columns[1:4])
            a.fillna(0,inplace = True)
            a.rename(columns={a.columns[0]: "VAR", a.columns[1]: "DT",a.columns[2]:"value"})
            a['Name'],a['ABC'],a['File'],a['DEF'],a['dir'] = mng_name,ABC,flst[i],DEF,f_list[i][0]
            a = a.values.tolist()
            date_prep(a,1)
            round_numbers(a,2)

            type_of_error = "Error THREE"
            df2 = pd.read_excel(lst[i], sheet_name='AMOUNT',usecols = 'C:G' ,skiprows = 2, nrows = 49)
            df2.drop(0, inplace=True)
            df2.dropna(subset=[df2.columns[0]], inplace=True)
            df3 = pd.read_excel(lst[i], sheet_name='AMOUNT',names = df2.columns,usecols = 'H:L' ,skiprows = 2, nrows = 49)
            df3.drop(0, inplace=True)
            df3.dropna(subset=[df3.columns[0]], inplace=True)
            df2_3 = pd.concat([df2,df3])
            df2_3 = pd.melt(df2_3,id_vars=df2_3.columns[0], VEvars=df2_3.columns[1:5])
            df2_3['Name'],df2_3['ABC'],df2_3['File'],df2_3['DEF'],df2_3['dir'] = mng_name,ABC,flst[i],DEF,f_list[i][0]
            df2_3.fillna(0,inplace = True)
            df2_3 = df2_3.replace(" ", 0)
            df2_3.reset_index(drop=True, inplace=True)
            df2_3 = df2_3.values.tolist()
            date_prep(df2_3,1)
            round_numbers(df2_3,2)

            type_of_error = "Error FOUR"
            df4 = pd.read_excel(lst[i], sheet_name='AMOUNT',usecols = 'O:W' ,skiprows = 2, nrows = 36)
            df4_1,df4_2 = df4.loc[:,list( df4.columns[i] for i in [0, 1, 3, 5, 7] )], df4.loc[:,list( df4.columns[i] for i in [0, 2, 4, 6, 8] )]
            df4_1.drop(0, inplace=True)
            df4_1.dropna(subset=[df4_1.columns[0]], inplace=True)
            df4_1.fillna(0,inplace = True)
            df4_2.drop(0, inplace=True)
            df4_2.dropna(subset=[df4_2.columns[0]], inplace=True)
            df4_2.fillna(0,inplace = True)
            df4_1['TYPE'],df4_2['TYPE'] = 'ABS','%'
            df4_1.columns,df4_2.columns = df4_1.columns.str.replace("\.2", ""),df4_1.columns.str.replace("\.2", "")
            df4 = pd.concat([df4_1,df4_2])
            df4.reset_index(drop=True, inplace=True)
            df4 = df4.replace(" ", 0)
            df4 = pd.melt(df4,id_vars=[df4.columns[0],df4.columns[-1]], VEvars=df4.columns[1:5])
            df4['Name'],df4['ABC'],df4['File'],df4['DEF'],df4['dir'] = mng_name,ABC,flst[i],DEF,f_list[i][0]
            df4 = df4[df4['value'].apply(lambda x: type(x)!=str)]
            df4 = df4.values.tolist()
            date_prep(df4,2)
            round_numbers(df4,3)

            type_of_error = "Error FIVE"
            print(type_of_error)
            df5 = pd.read_excel(lst[i], sheet_name='AMOUNT',usecols = 'Z:AD' ,skiprows = 2, nrows = 55)
            df5.drop(0, inplace=True)
            df5.dropna(subset=[df5.columns[0]], inplace=True)
            df5.fillna(0,inplace = True)
            df5.columns = df5.columns.str.replace("\.3", "")
            df5.reset_index(drop=True, inplace=True)
            df5 = df5.replace(" ", 0)
            df5 = pd.melt(df5,id_vars=[df5.columns[0]], VEvars=df5.columns[1:5])
            df5['Name'],df5['ABC'],df5['File'],df5['DEF'],df5['dir'] = mng_name,ABC,flst[i],DEF,f_list[i][0]
            df5 = df5.values.tolist()
            date_prep(df5,1)
            round_numbers(df5,2)

            type_of_error = "Error SIX"
            df6 = pd.read_excel(lst[i], sheet_name='AMOUNT',usecols = 'Z:AH' ,skiprows = 61, nrows = 21)
            df6.drop(0, inplace=True)
            df6.dropna(subset=[df6.columns[0]], inplace=True)
            df6.fillna(0,inplace = True)
            new_names = []
            for col in range(len(df6.columns)):
                if str(df6.columns[col]).startswith("Unnamed"):
                    new_names.append(df6.columns[col-1])
                else:
                    new_names.append(df6.columns[col])
            df6.columns = new_names
            df6_1 = df6.iloc[:, [0, 1, 3, 5, 7]]
            df6_2 = df6.iloc[:, [0, 2, 4, 6, 8]]
            df6_1.insert(1, "Type", 'Ratio')
            df6_2.insert(1, "Type", 'Score')
            df6 = pd.concat([df6_1,df6_2])
            df6 = df6.replace(" ", 0).replace(['A','B','C'], [1,2,3])
            df6 = pd.melt(df6,id_vars=[df6.columns[0],df6.columns[1]], VEvars=df6.columns[2:6])
            df6['Name'],df6['ABC'],df6['File'],df6['DEF'],df6['dir'] = mng_name,ABC,flst[i],DEF,f_list[i][0]
            df6 = df6[df6['value'].apply(lambda x: type(x)!=str)]
            df6 = df6.values.tolist()
            date_prep(df6,2)
            round_numbers(df6,3)

            type_of_error = "Error SEVEN"
            df7 = pd.read_excel(lst[i], sheet_name='AMOUNT',usecols = 'Z:AG' ,skiprows = 122, nrows = 11)
            df7.columns = ['Closure date'  if x==0 else df7.columns[x] for x in range(len(df7.columns))]
            df7.drop(0, inplace=True)
            df7.dropna(subset=[df7.columns[0]], inplace=True)
            df7.fillna(0,inplace = True)
            df7_1,df7_2 = df7[list( df7.columns[i] for i in [0, 1, 3, 5, 7] )], df7[list( df7.columns[i] for i in [0, 2, 4, 6] )]
            df7_1.columns = [str(col) for col in df7_1.columns]
            df7_1.columns = df7_1.columns.str.replace("\.1", "")
            df7_1 = pd.melt(df7_1,id_vars=[df7_1.columns[0]], VEvars=df7_1.columns[1:5])
            df7_2 = pd.melt(df7_2,id_vars=[df7_2.columns[0]], VEvars=df7_2.columns[1:4])
            df7_1["Type"],df7_2["Type"] = 'Meow',"said"
            df7 = pd.concat([df7_1,df7_2])
            df7['Name'],df7['ABC'],df7['File'],df7['DEF'],df7['dir'] = mng_name,ABC,flst[i],DEF,f_list[i][0]
            df7 = df7.values.tolist()
            date_prep(df7,1)
            round_numbers(df7,2)
 
            type_of_error = "Error EIGHT"
            df8_1 = pd.read_excel(lst[i], sheet_name='AMOUNT',usecols = 'Z:AC' ,skiprows = 89, nrows = 4)
            df8_1['DT'] = pd.read_excel(lst[i], sheet_name='AMOUNT',usecols = 'AA' ,skiprows = 86, nrows = 1).values[0][0]
            df8_2 = pd.read_excel(lst[i], sheet_name='AMOUNT',usecols = 'Z:AC' ,skiprows = 97, nrows = 4)
            df8_2['DT'] = pd.read_excel(lst[i], sheet_name='AMOUNT',usecols = 'AA' ,skiprows = 94, nrows = 1).values[0][0]
            df8_3 = pd.read_excel(lst[i], sheet_name='AMOUNT',usecols = 'Z:AC' ,skiprows = 105, nrows = 4)
            df8_3['DT'] = pd.read_excel(lst[i], sheet_name='AMOUNT',usecols = 'AA' ,skiprows = 102, nrows = 1).values[0][0]
            df8 = pd.concat([df8_1,df8_2,df8_3])
            df8.dropna(subset=[df8.columns[0]], inplace=True)
            df8.fillna(0,inplace = True)
            for hj in range(df8.shape[0]):
                if type(df8.iloc[hj,1]) == str:
                    df8.iloc[hj,1]=float(re.findall('\d*\,?\d+',df8.iloc[hj,1])[0].replace(',','.'))
            df8 = pd.melt(df8,id_vars=[df8.columns[0],df8.columns[4]], VEvars=df8.columns[1:4])
            df8['Name'],df8['ABC'],df8['File'],df8['DEF'],df8['dir'] = mng_name,ABC,flst[i],DEF,f_list[i][0]
            df8 = df8.values.tolist()
            date_prep(df8,1)
            round_numbers(df8,3)

            type_of_error = None
            print(type_of_error)

            try:
                shutil.move(lst[i], dir_ins)
            except BaseException:
                is_already_exist = 1
                shutil.move(os.path.join(os.path.dirname(lst[i]), flst[i]), os.path.join(dir_ins, flst[i]))

            if (is_already_exist == 0):
                con = cx_Oracle.connect(tns_str)
                cur = con.cursor()
                cur.executemany(rf"insert into A (YO,CD,VE,DLR,ABC,FLM,CATS,PATH) values (:1,:2,:3,:4,:5,:6,:7,:8)",a)
                cur.executemany(rf"insert into AA (YO,CD,VE,DLR,ABC,FLM,CATS,PATH) values (:1,:2,:3,:4,:5,:6,:7,:8)",df2_3)
                cur.executemany(rf"insert into AAA (YO, DIM, CD, VE, DLR, ABC, FLM, CATS, PATH) values (:1,:2,:3,:4,:5,:6,:7,:8,:9)",df4)
                cur.executemany(rf"insert into AAAA (YO, CD, VE, DLR, ABC, FLM, CATS, PATH) values (substr(:1,1,100),:2,:3,substr(:4,1,100),:5,substr(:6,1,100),:7,:8)",df5)
                cur.executemany(rf"insert into AAAAA (YO, INDC, CD, VE, DLR, ABC, FLM, CATS, PATH) values (:1,:2,:3,:4,:5,:6,:7,:8,:9)",df6)
                cur.executemany(rf"insert into AAAAAA (YO, CD, VE, INDC, DLR, ABC, FLM, CATS, PATH) values (:1,:2,:3,:4,:5,:6,:7,:8,:9)",df7)
                cur.executemany(rf"insert into AAAAAAA (YO, CD, STD, VE, DLR, ABC, FLM, CATS, PATH) values (:1,:2,:3,:4,:5,:6,:7,:8,:9)",df8)
                con.commit()
                con.close()

            i += 1
            k = 0

        except (NameError):
            shutil.move(os.path.join(os.path.dirname(lst[i]), flst[i]), os.path.join(dir_brk, flst[i]))
            insert_to_report("NameError")
            i += 1
            k = 0
            continue
            
        except(ValueError,KeyError):
            shutil.move(os.path.join(os.path.dirname(lst[i]), flst[i]), os.path.join(dir_brk, flst[i]))
            insert_to_report("ValueError,KeyError")
            i += 1
            k = 0
            continue
        except(biffh.XLRDError):
            shutil.move(os.path.join(os.path.dirname(lst[i]), flst[i]), os.path.join(dir_nl, flst[i]))
            insert_to_report("biffh.XLRDError")
            i += 1
            k = 0
            continue
        except(cx_Oracle.DatabaseError,cx_Oracle.OperationalError):
            if k<3:
                if k == 0:
                    pass
                k += 1
                continue
            else:
                shutil.move(os.path.join(os.path.dirname(lst[i]), flst[i]), os.path.join(dir_brk, flst[i]))
                insert_to_report("DB Connecting Error")
                i += 1
                k = 0
                continue
        except(TypeError):
            shutil.move(os.path.join(os.path.dirname(lst[i]), flst[i]), os.path.join(dir_brk, flst[i]))
            insert_to_report("TypeError")
            i+=1
            continue
        except(PermissionError):
            shutil.move(os.path.join(os.path.dirname(lst[i]), flst[i]), os.path.join(dir_brk, flst[i]))
            insert_to_report("PermissionError")
            i += 1
            continue
        finally:
            if (type_of_error != None):
                insert_to_report(type_of_error)
                shutil.move(os.path.join(os.path.dirname(lst[i]), flst[i]), os.path.join(dir_brk, flst[i]))
                i += 1

    elif  'Grass' in s_names:
        try:
            
            is_already_exist = 0
            
            dts = pd.read_excel(lst[i], sheet_name='Grass',usecols = 'D' ,skiprows = 0, nrows = 1).values[0][0]
            ts = (dts - np.datetime64('1970-01-01T00:00:00Z')) / np.timedelta64(1, 's')
            dts = dt.datetime.utcfromtimestamp(ts)
            rating =str(pd.read_excel(lst[i], sheet_name='Grass',usecols = 'D' ,skiprows = 5, nrows = 1).values[0][0])
            f_score = pd.read_excel(lst[i], sheet_name='Grass',usecols = 'D' ,skiprows = 30, nrows = 1).values[0][0]
            g_name =str(pd.read_excel(lst[i], sheet_name='Grass',usecols = 'C' ,skiprows = 1, nrows = 1).values[0][0])
            df1 = [(rating,round(f_score,4),g_name,flst[i],dts,f_list[i][0])]
            df2 = pd.read_excel(lst[i], sheet_name='Grass',usecols = 'A:F' ,skiprows = 38, nrows = 4)
            df2 = df2.loc[:,list( df2.columns[i] for i in [0, 2, 3, 5] )]
            df2 = df2.replace(" ", 0).replace(['OK'], [0])
            df2.dropna(subset = [df2.columns[0]], inplace=True)
            df2 = pd.melt(df2,id_vars=[df2.columns[0]], VEvars=df2.columns[1:4])
            df2.dropna(subset = [df2.columns[2]], inplace=True)
            df2['File'],df2['Dt'],df2['dir'] = flst[i],dts,f_list[i][0]
            df2 = df2.values.tolist()          
            round_numbers(df2,2)
            df3 = pd.read_excel(lst[i], sheet_name='Grass',usecols = 'A:F' ,skiprows = 23, nrows = 10)
            df3 = df3.loc[:,list( df3.columns[i] for i in [0, 2, 3, 5] )].rename(columns={df3.columns[2]: "Sunny"})
            df3.dropna(subset = [df3.columns[0]], inplace=True)
            df3 = pd.melt(df3,id_vars=[df3.columns[0]], VEvars=df3.columns[1:4])
            df3.dropna(subset = [df3.columns[2]], inplace=True)
            df3['File'],df3['Dt'],df3['dir'] = flst[i],dts,f_list[i][0]
            df3 = df3.values.tolist()
            round_numbers(df3,2)
            

            try:
              shutil.move(lst[i],dir_cats)
            except BaseException:
              is_already_exist = 1
              shutil.move(os.path.join(os.path.dirname(lst[i]),flst[i]),os.path.join(dir_cats, flst[i]))

                  
            if (is_already_exist == 0):
               con = cx_Oracle.connect(tns_str)
               cur = con.cursor()
               cur.executemany(rf"insert into B (FLOWERS, CLOUDS, OCEAN, FLM, KOGDA, PATH) values (:1,:2,:3,:4,:5,:6)",df1)
               cur.executemany(rf"insert into BB (ICE, KUDA, WIND, FLM, KOGDA, PATH) values (:1,:2,:3,:4,:5,:6)",df2)
               cur.executemany(rf"insert into BBB (BEE, KUDA, WIND, FLM, KOGDA, PATH) values (:1,:2,:3,:4,:5,:6)",df3)
               con.commit()
               con.close()
                    
            i+=1
            k=0
        except BaseException as e:
            print(str(e),'BaseException')
            insert_to_report(str(e))
            shutil.move(os.path.join(os.path.dirname(lst[i]),flst[i]),os.path.join(dir_brk, flst[i]))   
            i+=1

    else:
        shutil.move(os.path.join(os.path.dirname(lst[i]), flst[i]), os.path.join(dir_brk, flst[i]))
        i += 1