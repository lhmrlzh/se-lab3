import os
import pandas as pd
import mysql.connector
 
 
DEBUG = False

LABEL_TABLE = {
    "省份": ("province", "VARCHAR(255)", "NOT NULL"),
    "流域": ("basin", "VARCHAR(255)", "NOT NULL"),
    "断面名称": ("section_name", "VARCHAR(255)", "NOT NULL"),
    "监测时间": ("monitor_time", "DATETIME", "NULL"),
    "水质类别": ("water_quality_category", "VARCHAR(255)", "NULL"),
    "水温": ("water_temperature", "FLOAT", "NULL"),
    "pH": ("pH", "FLOAT", "NULL"),
    "溶解氧": ("dissolved_oxygen", "FLOAT", "NULL"),
    "电导率": ("conductivity", "FLOAT", "NULL"),
    "浊度": ("turbidity", "FLOAT", "NULL"),
    "高锰酸盐指数": ("permanganate_index", "FLOAT", "NULL"),
    "氨氮": ("ammonia_nitrogen", "FLOAT", "NULL"),
    "总磷": ("total_phosphorus", "FLOAT", "NULL"),
    "总氮": ("total_nitrogen", "FLOAT", "NULL"),
    "叶绿素α": ("chlorophyll_a", "FLOAT", "NULL"),
    "藻密度": ("algae_density", "FLOAT", "NULL"),
    "站点情况": ("station_status", "VARCHAR(255)", "NULL"),
}

NAME_TABLE = {
    "province": "省份",
    "basin": "流域",
    "section_name": "断面名称",
    "monitor_time": "监测时间",
    "water_quality_category": "水质类别",
    "water_temperature": "水温",
    "pH": "pH",
    "dissolved_oxygen": "溶解氧",
    "conductivity": "电导率",
    "turbidity": "浊度",
    "permanganate_index": "高锰酸盐指数",
    "ammonia_nitrogen": "氨氮",
    "total_phosphorus": "总磷",
    "total_nitrogen": "总氮",
    "chlorophyll_a": "叶绿素α",
    "algae_density": "藻密度",
    "station_status": "站点情况",
}


def connect_to_db(host="localhost", port="3306", database="ocean-monitor"):
    return mysql.connector.connect(
        host=host,
        port=port,
        user="root",
        password="114514",
        database=database,
        allow_local_infile=True,
    )


def create_table(cursor, table_name, columns):
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    if cursor.fetchone():
        print(f"Table {table_name} already exists. Skipping creation.")
        return

    columns_definition = ", ".join(
        [f"`{LABEL_TABLE[col][0]}` VARCHAR(255) {LABEL_TABLE[col][2]} COMMENT '{col}'" for col in columns]
    )
    create_table_query = f"CREATE TABLE `{table_name}` ({columns_definition})"
    if DEBUG:
        print(f"query: \n{create_table_query}")
    cursor.execute(create_table_query)
    print(f"Table {table_name} created successfully.")


def import_csv_to_table(cursor, table_name, csv_file):
    cursor.execute("SET @null_index = '--' COLLATE utf8mb4_0900_ai_ci")

    csv_file = csv_file.replace("\\", "/")
    cursor.execute(f"DESCRIBE `{table_name}`")
    table_columns = [row[0] for row in cursor.fetchall()]
    import_query = (
        f"LOAD DATA LOCAL INFILE '{csv_file}' "
        f"INTO TABLE `{table_name}` "
        f"CHARACTER SET utf8 "
        f"FIELDS TERMINATED BY ',' "
        f"LINES TERMINATED BY '\\r\\n' "
        f"IGNORE 1 LINES "
        f"({', '.join([f'{col}' for col in table_columns])}) "
        f"SET "
        f"{', '.join([f'{col} = NULLIF({col}, @null_index)' for col in table_columns])}"
    )
    if DEBUG:
        print(f"query: \n{import_query}")
    cursor.execute(import_query)
    print(f"Data from {csv_file} imported into table {table_name} successfully.")


def update_table_type(cursor, table_name):
    cursor.execute(f"DESCRIBE `{table_name}`")
    table_columns = [row[0] for row in cursor.fetchall()]

    for column in table_columns:
        if column in NAME_TABLE:
            label = NAME_TABLE[column]
            new_type = LABEL_TABLE[label][1]
            nullable = LABEL_TABLE[label][2]
            if new_type.startswith("DATETIME"):
                update_query = f"UPDATE `{table_name}` SET `{column}` = STR_TO_DATE(CONCAT(LEFT('{table_name}', 4), '-', `{column}`), '%Y-%m-%d %H:%i') WHERE `{column}` IS NOT NULL"
                cursor.execute(update_query)
                alter_query = f"ALTER TABLE `{table_name}` MODIFY COLUMN `{column}` {new_type} {nullable}"
            else:
                alter_query = f"ALTER TABLE `{table_name}` MODIFY COLUMN `{column}` {new_type} {nullable}"
            cursor.execute(alter_query)
            print(f"Column {column} in table {table_name} updated to type {new_type}.")


def main(input_dir):
    connection = connect_to_db(database="ocean-monitor")
    print("Database connection successful.")
    cursor = connection.cursor()
    for root, _, files in os.walk(input_dir):
        sorted(files)
        flag = False
        for file in files:
            if file.endswith(".csv"):
                flag = True
                dir = os.path.basename(root)
                csv_file = os.path.join(root, file)
                print(f"正在处理文件: {csv_file}")
                df = pd.read_csv(csv_file)
                columns = df.columns.tolist()

                create_table(cursor, dir, columns)
                import_csv_to_table(cursor, dir, csv_file)
        if flag:
            update_table_type(cursor, dir)

    connection.commit()
    cursor.close()
    connection.close()


input_dir = "data/水质数据"
if __name__ == "__main__":
    main(input_dir)
