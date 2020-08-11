#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import shutil

# 所有表
#table_list=["CST_agentno_rela",
#"CST_prod_rela",
#"agentno_prod_rela",
#"agentno_property",
#"chnl_agentno_rela",
#"chnl_code_property",
#"chnl_manager_chnl_rela",
#"chnl_manager_district_rela",
#"chnl_manager_prod_rela",
#"chnl_manager_property",
#"chnl_prod_rela",
#"cst_property",
#"district_chnl_rela",
#"district_prod_rela",
#"district_property",
#"fund_manager_property",
#"fund_prod_property",
#"manager_prod_rela"
#]

# 配置本地缓存csv文件的路径
tmp_dir_path="/home/ec2-user/tmpcsv"
# 配置neo4j 执行导入的路径
target_path="/home/ec2-user/neo4j-community-4.1.1/import"

# 初始化时间
#date=""
#flag_n = 0

# 从S3 同步数据, 增量同步,只传输变化的文件, 并删除本地多余的文件
print("开始从S3同步到本地%s ..." %(tmp_dir_path))
os.system("aws s3 sync s3://bilab-ads/knowledge_graph   /home/ec2-user/tmpcsv  --exclude \"*folder*\" --delete ")

# 本地目录 处理
print("同步结束,开始拷贝到指定目录%s ..." %target_path )


#获取 tmpcsv 目录 最新的所有表 /home/ec2-user/tmpcsv
table_list=os.listdir(tmp_dir_path)


def format_p_dt(p_dt_list):
    date_list=[]
    for p_dt in p_dt_list:        
        date_list.append(p_dt.split('=')[1])
    return date_list

#def copy_files():
#    pass

#dict1={"20200717":["aaa","bbb","ccc"]}
#print(dict1["20200717"].pop())
#print(dict1["20200717"].pop())
#print(dict1["20200717"].pop())
#print(dict1["20200717"].pop())
date_tab_dict={}
#### 遍历每一个表
for tab in table_list:
    #flag_n+=1
    # 获取每个表的日期列表
    p_dt_list=os.listdir(tmp_dir_path+"/"+tab)
    date_list=format_p_dt(p_dt_list)
    # 每个表的多个日期
    for date in date_list:       
        #判断每个日期下 表里的文件数
        tmp_file_path=tmp_dir_path+"/"+tab+"/p_dt="+date
        #print(tmp_file_path)
        file_name_list=os.listdir(tmp_file_path)
        if len(file_name_list) ==1 :
            date_tab_dict.setdefault(date,[]).append(tab)
            #print(date_tab_dict)    

        elif len(file_name_list) ==0:
            print("error:目录%s下,csv文件不存在!"  %tmp_file_path )
            os._exit()
        else :
            
            print("error:目录%s下,存在多个csv文件!"  %tmp_file_path ) 
            os._exit()

###  查看每个key 对应list中的元素个数
print("遍历结束. date_tab_dict:")
for key in sorted(date_tab_dict.keys()):
    print(key ,len(date_tab_dict[key]))

### 日期,表 
for date in sorted(date_tab_dict.keys()):
    if date in ["20200717","20200724"]:
        # 判断neo4j 对应目录是否存在
        if  (os.path.exists(target_path+"/"+date)):
            print(target_path+"/"+date+" 目录 已存在!")
        else:
            #创建目录
            os.mkdir(target_path+"/"+date)
            #
            flag_n=0
            #开始传输
            for tab in date_tab_dict[date]:
                tmp_file_path=tmp_dir_path+"/"+tab+"/p_dt="+date
                file_name_list=os.listdir(tmp_file_path)
                f_full_path=tmp_file_path+"/"+file_name_list[0]
                #
                target_full_path_withouttpye=target_path+"/"+date+"/"+tab
                target_full_path=target_full_path_withouttpye+".tmp"
                shutil.copyfile(f_full_path,target_full_path)
                # 处理文本, 添加 p_dt 列
                os.system("awk '{print $0 \",%s\"}' %s >%s.csv && sed -i -e '1s/%s/p_dt/'  %s.csv && rm -f %s " %(date, target_full_path,target_full_path_withouttpye, date, target_full_path_withouttpye,target_full_path) )
                # awk '{print $0 ",20200717"}' agentno_property.csv > 1.csv  &&  sed -i -e '1s/20200717/p_dt/'  1.csv   && rm -f 1.csv
                flag_n+=1
                print("%s--%d. [%s] 拷贝完成!" %(date,flag_n,tab) )
                #
            ## 
            print("[%s]开始更新cypher脚本日期  ..." %date)
            # 修改cypher 脚本的日期
            os.system("sed -i -e 's/^\(:param date=>\"\)\(.*\)\(\"\)/\\1%s\\3/' cypher.script " %date)
            print("[%s]开始执行cypher-shell ..." %date)
            # cypher-shell / 执行脚本
            os.system("/home/ec2-user/neo4j-community-4.1.1/bin/cypher-shell  -u neo4j -p 12345678 -f /home/ec2-user/cypher.script ") 
            print("[%s]执行完毕 ..." %date)



#当neo4j对应的日期目录不存在时,才会导入数据
#        if flag_n==1 and not (os.path.exists(target_path+"/"+date)):
#            #创建目录
#            os.mkdir(target_path+"/"+date)
#        elif flag_n==1  and (os.path.exists(target_path+"/"+date)):
#            print("日期:%s 的目录已存在!"%())    
#            break    
#
#        #print(file_name_list)            

#print("开始更新cypher脚本日期 ...")
# 修改cypher 脚本的日期
#os.system("sed -e 's/^\(:param date=>\"\)\(.*\)\(\"\)/\\1%s\\3/' cypher.script " %date)

#print("开始执行cypher-shell ...")
# cypher-shell / 执行脚本
#os.system("/home/ec2-user/neo4j-community-4.1.1/bin/cypher-shell  -u neo4j -p 12345678 -f /home/ec2-user/cypher.script ") 
#print("执行完毕 ...")