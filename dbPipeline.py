import pymysql
from warnings import filterwarnings
filterwarnings('ignore', category = pymysql.Warning)


class mysqlPipe():
    def __init__(self,name,host='localhost',port=3306,user='root',passwd='Zhou195856',db='test'):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        self.name = name#指定网站的名字
        try:
            self.connect_obj = pymysql.connect(self.host, self.user, self.passwd, self.db)
            cursor = self.connect_obj.cursor()
            sql_inquery = """
                        create table if not exists inquery_online_%s
                        (
                          问诊编号 int unsigned auto_increment,
                          医生编号 varchar(100)   null,
                          患者性别 varchar(4)    null,
                          患者年龄 int           null,
                          问诊内容 varchar(1000) null,
                          primary key (问诊编号)
                        )"""%self.name
            sql_doctor = """
                        create table if not exists doctor_info_%s
                        (
                            医生编号 varchar(20) not null,
                            医生科室 varchar(20) null,
                            医生职称 varchar(20) null,
                            所属医院 varchar(20) null,
                            擅长疾病 varchar(60) null,
                            primary key (医生编号)   
                        ) """%self.name
            sql_department = """
                        create table if not exists department_info_%s
                        (
                           科室名称 varchar(20) not null,
                            科室疾病 varchar(2000) null,
                            primary key (科室名称)   
                        ) """%self.name
            #事务处理
            try:
                cursor.execute(sql_inquery)
                cursor.execute(sql_doctor)
                cursor.execute(sql_department)
                cursor.close()
            except:
                self.connect_obj.rollback()
                print("创建数据表时发生异常")
            else:
                self.connect_obj.commit()
                print("数据表自动创建成功")
        except:
            print("尝试连接以下数据库失败:\nhost:%s\nport:%d\nuser:%s\ndatabase:%s"%(self.host,self.port,self.user,self.db))

    def insert_doctor_info(self,id,department,title,hospital,goodat):
        try:
            cursor = self.connect_obj.cursor()
            cursor.execute("insert ignore into doctor_info_%s VALUES ('%s','%s','%s','%s','%s')"%
                           (self.name,id, department, title, hospital, goodat))
            cursor.close()

        except Exception as e:
            self.connect_obj.rollback()
            print("插入医生信息失败:%s" % (id))
            print(e)
        else:
            self.connect_obj.commit()
            print("插入医生信息成功:%s " % (id))

    def insert_department_info(self,department_name,diseases):
        try:
            cursor = self.connect_obj.cursor()
            cursor.execute("insert ignore into department_info_%s values ('%s','%s')"%
                           (self.name,department_name, diseases))
            cursor.close()

        except:
            self.connect_obj.rollback()
            print("插入科室信息失败:%s" % (department_name))
        else:
            self.connect_obj.commit()
            print("插入科室信息成功:%s" % (department_name))

    def insert_inquery_online(self,doc_id,sex,age,inquery_content):
        try:
            cursor = self.connect_obj.cursor()
            cursor.execute("insert into inquery_online_%s (医生编号,患者性别,患者年龄,问诊内容) VALUES ('%s','%s',%d,'%s')"%
                           (self.name,doc_id, sex, age, inquery_content))
            cursor.close()

        except Exception as e:
            self.connect_obj.rollback()
            print(e)
            print("插入问诊信息失败 %s %s %d"%(doc_id,sex,age))
        else:
            self.connect_obj.commit()
            print("插入问诊信息成功")
