import pandas as pd
import pymysql


# 读取excel中的所有sheet
def getSheets(path, sheet_name=None):
    # 读取所有sheet，sheet_name = None用于读取所有sheet(header从哪行开始读取数据，默认为0是从第一行开始，usecols = 'B:F'指读取B - -F列）
    datas = pd.read_excel(path, sheet_name=None)
    # dataframe类型的columns和keys()返回内容类型都是index类型数据,可以直接转list、tuple，或者使用values获得列表类型的值
    sheet_names = tuple(datas.keys())
    return sheet_names


# 从指定行获取字段列名称，并逐个读取sheet中的所有数据
def getSheetDatas(sheet_name):
    # header 指定作为列名的行，默认0，即取第一行的值为列名；若数据不包含列名，则设定 header = None
    datas = pd.read_excel(path, sheet_name=sheet_name, header=3)
    ## 在pandas中空值以nan展示，当写入数据库时的空值需要转换为None
    datas = datas.fillna(value='None')
    # 获取所有列dataframe类型的columns和keys()返回内容类型都是index类型数据，可以直接转list、tuple，或者使用values获得列表类型的值(下面2行语句返回相同)
    columns = datas.keys().values
    # columns = list(datas.keys())
    # columns = datas.columns.values

    # 每次迭代提取DataFrame类型的值，并转换为list类型
    datas = (datas.values).tolist()
    return columns, datas


# 按照sheet依次写入mysql数据库
def writeToMysql(datas):
    conn = pymysql.connect(host='localhost', user='root',
                           password='Mysqlroot', database='0220testing', port=3306)
    cur = conn.cursor()
    # 判断表是否存在
    cur.execute("show tables")
    tables_tuple = cur.fetchall()
    # 创建表sql
    createTable = """CREATE TABLE `housedetail` (
                      `areaNames` varchar(255) DEFAULT NULL,
                      `totalPrices` varchar(255) DEFAULT NULL,
                      `secPrices` varchar(255) DEFAULT NULL,
                      `mianJis` varchar(255) DEFAULT NULL,
                      `jiaoYiGuanShus` varchar(255) DEFAULT NULL,
                      `createTimes` varchar(255) DEFAULT NULL,
                      `areas` varchar(255) DEFAULT NULL,
                      `areaDetails` varchar(255) DEFAULT NULL,
                      `huXings` varchar(255) DEFAULT NULL,
                      `louCengs` varchar(255) DEFAULT NULL,
                      `jieGous` varchar(255) DEFAULT NULL,
                      `jianZhuLeiXings` varchar(255) DEFAULT NULL,
                      `chaoXiangs` varchar(255) DEFAULT NULL,
                      `jianZhuJieGous` varchar(255) DEFAULT NULL,
                      `zhuangXius` varchar(255) DEFAULT NULL,
                      `tiHuBiLis` varchar(255) DEFAULT NULL,
                      `guaPaiTimes` varchar(255) DEFAULT NULL,
                      `fangWuYongTus` varchar(255) DEFAULT NULL,
                      `lastJiaoYis` varchar(255) DEFAULT NULL,
                      `houseNianXians` varchar(255) DEFAULT NULL,
                      `belongTos` varchar(255) DEFAULT NULL,
                      `diYas` varchar(255) DEFAULT NULL,
                      `fangBens` varchar(255) DEFAULT NULL
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"""
    # 如果返回元祖长度为0就创建表
    if len(tables_tuple) == 0:
        cur.execute(createTable)
    # 如果返回元祖长度不为0且表名称不存在时创建表
    if len(tables_tuple) != 0:
        for itme in tables_tuple:
            if 'housedetail' not in itme:
                cur.execute(createTable)
    try:
        # 存在重复数据时进行更新操作，联合索引的各字段长度的总和需要注意，对于重复值需要数据表定义好键和索引；
        sql = "insert into housedetail (areaNames, totalPrices, secPrices, mianJis, jiaoYiGuanShus, createTimes, areas, \
        areaDetails, huXings, louCengs, jieGous, jianZhuLeiXings, chaoXiangs, jianZhuJieGous, zhuangXius, tiHuBiLis, \
        guaPaiTimes, fangWuYongTus, lastJiaoYis, houseNianXians, belongTos, diYas, fangBens) values \
        (%s, %s, %s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s) on duplicate key update \
        areaNames=values(areaNames), totalPrices=values(totalPrices), secPrices=values(secPrices), mianJis=values(mianJis), \
        jiaoYiGuanShus=values(jiaoYiGuanShus), createTimes=values(createTimes), areas=values(areas), areaDetails=values(areaDetails), \
        huXings=values(huXings), louCengs=values(louCengs), jieGous=values(jieGous), jianZhuLeiXings=values(jianZhuLeiXings), \
        chaoXiangs=values(chaoXiangs), jianZhuJieGous=values(jianZhuJieGous), zhuangXius=values(zhuangXius), tiHuBiLis=values(tiHuBiLis), \
        guaPaiTimes=values(guaPaiTimes), fangWuYongTus=values(fangWuYongTus), lastJiaoYis=values(lastJiaoYis), \
        houseNianXians=values(houseNianXians), belongTos=values(belongTos), diYas=values(diYas), fangBens=values(fangBens)"

        # 添加的数据datas的格式必须为list[tuple(),tuple(),tuple()]或者tuple(tuple(),tuple(),tuple())
        cur.executemany(sql, datas)
        conn.commit()
        print('Insert Successful'.center(60, '-'))
        datas = cur.fetchall()
    except Exception as e:
        print('Fialed'.center(60, '~'))
        print(e.args)
    finally:
        cur.close()
        conn.close()
    # return datas


if __name__ == '__main__':

    path = r'D:\housedetail.xlsx'

    sheet_names = getSheets(path, sheet_name=None)

    for sheet_name in sheet_names:
        columns, datas = getSheetDatas(sheet_name)

        writeToMysql(datas)
