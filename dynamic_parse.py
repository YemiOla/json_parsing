
import boto3
import json
import pg8000.core
import logging
import pandas as pd
import pg8000.native as pg
import ssl
import ast
from airflow.models.baseoperator import BaseOperator
# from typing import (
#     ClassVar,
#     List,
#     Any,
#     Tuple
# )

class DynamicParse(BaseOperator):
    """description here"""

    template_fields = ['dataset_date']

    def __init__(
        self,
        s3_path_app_oper: str,
        dataset_date: str,
        region_name: str = 'us-east-1',
        **kwargs 
    ) -> None:
        super().__init__(**kwargs)

        self.s3_path_app_oper=s3_path_app_oper
        self.dataset_date = dataset_date
        self.region_name = region_name

    




    def connect(self):
        """Connects to hercules reporting cluster"""
        # STS duration seconds defaults to 1 hour

        PORT = xxxx
        DATABASE = 'xxxx'
        REDSHIFT_ROLE  = 'xxxxx'
        REDSHIFT_HOST  = 'xxxxx' # 
        SSL_CERTIFICATES = 'xxxxxx'
        CLUSTER_IDENTIFIER = 'xxxxxx'

        response = boto3.client('sts').assume_role(
            RoleArn = REDSHIFT_ROLE,
            RoleSessionName = 'xxxx'
        )
        client_credentials = response['Credentials']
        client = boto3.client(
            'redshift',
            region_name = 'us-east-1',
            aws_access_key_id = client_credentials['AccessKeyId'],
            aws_secret_access_key = client_credentials['SecretAccessKey'],
            aws_session_token = client_credentials['SessionToken'],
        )
        logging.info(f"STS token of assumed role expires on: {client_credentials['Expiration']}")
        response = client.get_cluster_credentials(
            DbUser = 'xxxxx',
            DbName = DATABASE,
            ClusterIdentifier = CLUSTER_IDENTIFIER,
            AutoCreate = False,
            DurationSeconds = 60 * 60,
        )

        # Trust required certificates to connect to Redshift with SSL
        ssl_context = ssl.SSLContext()
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        ssl_context.load_verify_locations(SSL_CERTIFICATES)
        logging.info(f"Verified Redshift Certificate Authorities to enable SSL connection")
        logging.info(f"Trying to connect to {REDSHIFT_HOST} PORT: {PORT}")

        connection = pg8000.connect(
            database = DATABASE,
            host = REDSHIFT_HOST,
            port = PORT,
            user = response['DbUser'],
            password = response['DbPassword'],
            ssl_context = ssl_context
        )
        logging.info("Successfully connected to redshift via pg8000")
        return connection



    def get_tables(self, path):
        """This will grab the table_list txt file. Table_list contains the name of the tables we want to query from.
            It will take the file path as a function and return as a dataframe only the tables that are turned ON.
        """
        s3_client = boto3.client(
        "s3"
        )

        response = s3_client.get_object(Key=path, Bucket="mwaa-source-code")

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}")
            tables = pd.read_csv(response.get("Body"), sep = ":")
            
            return tables.query('to_be_queried =="yes"')



    def get_opers_and_name_of_table_load(self, path):
        """ This will grab app_oper groups text file. This file contains the names of the different operations and the table names of the
            data of the different operations that would be uploaded.
            The parameter passed would be the file path.- It will return the dataframe of only the tables that are turned ON.
        """
        s3_client = boto3.client(
        "s3"
        )

        response = s3_client.get_object(Key=path, Bucket="mwaa-source-code")

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}")
            oper_table = pd.read_csv(response.get("Body"), sep = ":")
            # print(oper_table)
            return oper_table.query('to_be_queried =="yes"')






    def flatten_nested_json_df(self, df):
        """This function would help to parse the dataframe and unnest any nested jsons.
            Explode dictionaries horizontally using json_normalize creating new columns with the dict keys. Also add the name
            of the column it is being exploded from as prefix. This helps for tracebility, debugging and ensure consistent 
            column naming nomenclature. 
            It explodes the lists vertically creating new rows.
            
        """

        df = df.reset_index()

        

        # search for columns to explode/flatten based on the data type of the columns.
        s = (df.applymap(type) == list).all()
        list_columns = s[s].index.tolist()

        s = (df.applymap(type) == dict).all()
        dict_columns = s[s].index.tolist()

        logging.info(f"lists: {list_columns}, dicts: {dict_columns}")
        while len(list_columns) > 0 or len(dict_columns) > 0:
            new_columns = []

            for col in dict_columns:
                logging.info(f"flattening: {col}")
                
                horiz_exploded = pd.json_normalize(df[col]).add_prefix(f'{col}_')
                horiz_exploded.index = df.index
                df = pd.concat([df, horiz_exploded], axis=1).drop(columns=[col])
                new_columns.extend(horiz_exploded.columns)

            for col in list_columns:
                logging.info(f"exploding: {col}")
                df = df.explode(f"{col}")                      
            
                
                new_columns.append(col)
                logging.info(f"finished exploding: {col}")       

        

            #There are some columns with a combination of NaN and list. The NaN type is a float, hence 
            #we will convert the NaN into string by filling it with "No_data" and then replace it with an
            #empty list. This would thus make the column a list data type.
            s = (df.applymap(type) == list).any()
            list_columns = s[s].index.tolist()
            for col in list_columns:
                df[col].fillna('No_data', inplace = True)
                df[col] = [[] if x == 'No_data' else x for x in df[col]]

            #There are some columns with a combination of NaN and dict. The NaN type is a float, hence 
            #we will convert the NaN into string by filling it with "No_data" and then replace it with an
            #empty dict. This would thus make the column a dict data type.
            s = (df.applymap(type) == dict).any()
            dict_columns = s[s].index.tolist()
            for col in dict_columns:
                df[col].fillna('No_data', inplace = True)
                df[col] = [{} if x == 'No_data' else x for x in df[col]]


            #There are some columns with a combination of NaN and a str representation of list which in turn contains string representaion of dict. The NaN type is a float, hence 
            #we will convert the NaN into string by filling it with "No_data" and then replace it with a string representaion of an
            #empty list. We would then remove the opening and closing square brackets in the column so as to expose the internal list representation of dict. 
            
           
            s = (df.applymap(type) == str).any()
            str_columns = s[s].index.tolist()
            for col in str_columns:
                df[col] = df[col].fillna('No_data')
                df[col] = [ '[]' if x == 'No_data' else x for x in df[col]]
                df[col] = df[col].apply(lambda x: x.strip('['))
                df[col] = df[col].apply(lambda x: x.strip(']'))
                # try:
                #     df[col] = df[col].apply(self.json_str_to_dict)
                # except (ValueError, TypeError):
                #     pass

            
                        
            # check if there are still dict or list fields/columns to flatten
            s = (df[new_columns].applymap(type) == list).all()
            list_columns = s[s].index.tolist()

            s = (df[new_columns].applymap(type) == dict).all()
            dict_columns = s[s].index.tolist()                  
                
        

            logging.info(f"lists: {list_columns}, dicts: {dict_columns}")
            len_of_list_columns = len(list_columns)
            len_of_dict_columns = len(dict_columns)
            logging.info(f"Length of List Columns, {len_of_list_columns}")
            logging.info(f"Length of dict Columns, {len_of_dict_columns}")

            
        return df





    def json_str_to_dict(self, d):
        """
        Convert json string into a python dictionary. 
        You need to pass in a json string and it would return a python dict
        """
        
        mydict = json.loads(d)
        return mydict


    def json_str_to_dict_invalid_escape(self, d):
        """
        Convert json string that has presence of invalid escape into a python dictionary.
         This function is implemented due to this error (json.decoder.JSONDecodeError: Invalid \escape:)
         gotten when parsing json of operations EXECUTION_REGISTRATION, EXECUTION_START and EXECUTION_COMPLETION 
        You need to pass in a json string and it would return a python dict
        """
        mydict = ast.literal_eval(d)
        return mydict


    
    def read_table(self, table_name, operations, last_x_day = 1, limit = 0):
        """This function would help to read the table to be queried from.

            As parameters, it would take:
            table_name  = This table name would be gotten from the table_list.txt.
            operations = This operations would be gotten from the app_oper groups text file

            If limit is set to 0, we will query the whole dataset otherwise the number of rows of data that would be pulled 
            would be the value set at the limit.
        Hence, if we don't pass a limit, the limit is set to 0, and all data is queried.
        """
          
        connection = None
        
        # connection = connect_to_db(dbparam["database"], dbparam["host"],dbparam["user"],dbparam['password'],dbparam["port"])
        connection = self.connect()

        try:
        # connection.autocommit = True
            cursor = connection.cursor()
        except Exception as err:
            logging.info(f"Cursor not activated!. Check the error statement: {err}")
        
        if limit == 0:
            query = f''' SELECT * FROM {table_name} WHERE operation IN ({operations}) AND DATE(publishing_time) = DATE('{self.dataset_date}') '''
            logging.info(query)
        else:
            query = f''' SELECT * FROM {table_name} WHERE operation IN ({operations}) AND DATE(publishing_time) = DATE('{self.dataset_date}') LIMIT {limit} '''
            logging.info(query)
        # cursor.execute(query)
        # data = cursor.fetchall()
        

        #Extract the amh_user_activity_prod_2 table into a dataframe
        df_b4_processing = pd.read_sql_query(query, connection)
        
        return df_b4_processing, cursor, connection
        
    def df_flatten_explode(self, df_b4_processing, cursor, connection, operations_as_list):
        #The application_context column in the df is a json string and would need to be converted to a python dict
        
        
        
        if "'EXECUTION_REGISTRATION'" in operations_as_list or "'EXECUTION_START'" in operations_as_list or "'EXECUTION_COMPLETION'" in operations_as_list:
        
            df_b4_processing['application_context'] = df_b4_processing['application_context'].apply(self.json_str_to_dict_invalid_escape)
            
        else:
            df_b4_processing['application_context'] = df_b4_processing['application_context'].apply(self.json_str_to_dict)


        #Apply this function to unnest the df. It will flatten the dictionary and explode the lists.
        logging.info('Processing the json data begins...')
        df = (self.flatten_nested_json_df(df_b4_processing))
        

        #A new column called index formed during processing above using flatten_nested_json_df. This needs to be dropped along the column axis.
        df.drop('index', axis = 1, inplace = True)
        logging.info('Processing the json data completed...')
        
        col_names_list = df.columns

        #We would like to change the columns to lower case so that we can easily use them in the function check_if_col_diff.
        #This helps to avoid any surprises of upper case vs lower case in the column names.
        col_names_list = [col.lower() for col in col_names_list]

        
        #We want to ensure that there are no periods and dash in the column names otherwise it might cause some error when creating and/or uploading database table.
        #Also we want to ensure that the length of our column name is not more than 127 characters otherwise redshift will complain.
        col_names_list_und = []
        for col in col_names_list:
            col = col.replace(".", "_")
            col = col.replace("-", "_")
            if len(col) >= 127:
                diff_in_length = len(col) - 127
                col = col[diff_in_length:]
                col_names_list_und.append(col)
            else:
                col_names_list_und.append(col)

            
                
            
        #Assign the newly formed column names to the dataframe
        df.columns = col_names_list_und

        col_names_list_updated = df.columns
        
        
        #Convert the list of columns to tuple. This would be needed when building our INSERT query.
        col_names_tuple = tuple(col_names_list_updated)
        

        
        return df, col_names_list_updated, cursor, connection, col_names_tuple



    def read_already_existed_dbtable(self, connection, cursor, table_name_to_load, limit = 1):
        """This function would read already existed database table so we can have access 
            to the columns. We would only do a limit of 1 since we do not actually need 
            the data but only need the column names.
        """  
        
        
        query = f'SELECT * FROM {table_name_to_load} LIMIT {limit}'   

        #Extract the table_name_to_load table into a dataframe
        df_from_dbtable = pd.read_sql_query(query, connection)       
            
        
        #We would like to change the columns to lower case so that we can easily use them in the function check_if_col_diff.
        #This helps to avoid any surprises.
        col_names = df_from_dbtable.columns
        col_names = [col.lower() for col in col_names]

           
        
        return df_from_dbtable,  cursor, connection, col_names


    def check_if_col_diff(self, col_names_list_updated, col_names):
        """This will check the difference in the columns of the database table to be loaded and the 
            tables of the processed data from the user_activity_table.
            Specifically, it will return the list of columns that are in the user_activity_table but not in the already created table.
        """
        d_diff = [col for col in col_names_list_updated if col not in col_names]        
        
        return d_diff



    def create_db_dynamically(self, df, table_name_to_load, cursor):
        """This function will create the table in the database if table does not already exist.
        All columns would be made of varchar datatype on the staging table to be created.

        Parameters:
        df: This would be dataframe returned from the read_table function
        table_name_to_load: obtained from the app_oper group text files
        cursor = This is returned from the read table function.    
        """

        SQL_CREATE_TBL = f'''CREATE TABLE IF NOT EXISTS {table_name_to_load} ('''
        
        for i in range(0, len(df.columns)):
            SQL_CREATE_TBL += "{} varchar(max), ".format(df.columns[i])
        #The SQL_CREATE_TBL string has a trailing comma after the varchar(max) keyword for the last column and would have to be removed.
        SQL_CREATE_TBL = SQL_CREATE_TBL.rstrip(" ,")
        #Now append the closing parenthesis and semicolon to the SQL_CREATE_TBL variable.
        SQL_CREATE_TBL += ");"
        logging.info(SQL_CREATE_TBL)

        #It doesn't need to return anything. Just execute.
        cursor.execute(SQL_CREATE_TBL)



   


    
    def update_dbtable_with_new_col(self, table_name_to_load, d_diff, cursor, col_names_list_updated, col_names):
        """check the difference in the columns of the database table to be loaded and the 
        tables of the processed data from the user_activity_table.
        If there is a difference, then we alter the table to add the extra column(s).
        """  
             
        for i in range(0, len(d_diff)):            
            SQL_ALTER_TBL = f'''ALTER TABLE {table_name_to_load} '''
            SQL_ALTER_TBL += "ADD COLUMN {} varchar(max);".format(d_diff[i])
            logging.info(SQL_ALTER_TBL)
            cursor.execute(SQL_ALTER_TBL)
            
                            
                  
   

    def build_insert_query(self, table_name_to_load, col_names_tuple):
        """This will build the insert query statement to be used when updating the database table.
        """
        #Convert the col_names_tuple into a string so that it doesn't throw error in the INSERT INTO statement
        #since the INSERT into statement accepts a string without quotes.
        col_names_string = ','.join(col_names_tuple)

        #Let us iterate through the col_names_tuple and then replace each column name with %s
        # to get a placeholder for the column_values
        # column_values = tuple(map(lambda column: column.replace(column, '%s::varchar(max)'), col_names_tuple))
        column_values = tuple(map(lambda column: column.replace(column, '%s'), col_names_tuple))
        #Convert the column_values into a string so that it doesn't throw error in the INSERT INTO statement
        #since the INSERT INTO statement accepts %s without quotes.
        column_values_string = ','.join(column_values)

        # query = f'''
        # INSERT INTO {table_name_to_load} ({col_names_string}) VALUES ({column_values_string})  
        # '''
        query = f'''
        INSERT INTO {table_name_to_load} ({col_names_string})  
        '''
        # logging.info(query)
        return query



    




    def get_unique_id(self, table_name_to_load, connection, last_x_day=1):
        query = f''' SELECT id FROM {table_name_to_load} WHERE DATE(publishing_time) = DATE('{self.dataset_date}') '''
        logging.info(query)
        if_data_exists_df = pd.read_sql_query(query, connection)
        unique_id = if_data_exists_df['id'].unique()
        unique_id = set(unique_id)
        return unique_id




    def insert_data_into_db(self, connection, cursor, unique_id, query, df, col_names_tuple, batch_size = 1000):
        """Check to see if the id exists in the database, if it does, continue (i.e do not do anything) 
            otherwise if it does not, grab the data and append/insert it to the existing table.
            Uploading would be done in patches.
        """
        # logging.info(f'Total length of rows to load is: {len(df)}')
        # logging.info(f'Total length of columns to load is: {len(df.columns)}')
        logging.info("Just entered insert_data_into_db")
       
        count= 0
        for index, row in df.iterrows():
            execute_query = f'''{query} VALUES ('''        
            
            if row['id'] in unique_id:
                continue              
            
            
            
            for cols in col_names_tuple:
                
                row[cols] = str(row[cols])
                row[cols] = row[cols].replace("'", "''")
                execute_query += " '{}',".format(row[cols])
            execute_query = execute_query.rstrip(" ,")
            execute_query += ");"                  
            cursor.execute(execute_query)            
            count = count + 1
            logging.info(f'Executed {count} rows')
            if count % batch_size == 0:
                connection.commit()
                logging.info(f'We have now commited a total of {count} rows')
                
                
        
        connection.commit()
        logging.info(f'Finally! We commited a total of {count} rows')


      
        
    



    def load_table(self, connection, cursor, df, col_names_tuple, table_name_to_load,unique_id):
        """The function is to orchestrate the loading of data into the database table using other created functions.
        """
        query = self.build_insert_query(table_name_to_load, col_names_tuple)
        self.insert_data_into_db(connection, cursor, unique_id, query, df, col_names_tuple, batch_size = 1000)
        connection.close()





    def execute(self, context):
        """This is the main function running other functions.
        """
        
        logging.info('Application Starting...')
        #This would give us the table that is turned ON to be queried as dataframe
        tables = self.get_tables('dags/operators/app_context_dynamic_parser/table_list.txt')
        
        #Grab the operations and table to load that are turned ON as dataframe
        oper_tab_to_load_list_df = self.get_opers_and_name_of_table_load(self.s3_path_app_oper)
        connection = self.connect()
        

        #Now let us iterate through the schema_table_name column in the  tables dataframe so as to grab the table name to be queried from and then iterate through the 
        # oper_tab_to_load_list_df so as to have access to the operations to be pulled and the table name to upload the data.
        for table_name in tables['schema_table_name']:
            if table_name == "stg_usr.amh_user_activity_prod_2":         
                for operations_table_to_load in oper_tab_to_load_list_df['oper_table_combine']:
                    #operations_table_to_load is a str, hence we need to convert to tuple so as to grab the operations and 
                    #the table name to load to using index slicing.
                    operations_table_to_load_tup = tuple(map(str, operations_table_to_load.split(', ')))

                    #Now let us grab the operations as tuple and then convert to string by using .join so it can be used in the query.
                    operations = operations_table_to_load_tup[:-1]                   
                    operations = ",".join(operations)

                    #Now extract the name of the table to load by indexing. Since this is only one index, it outputs as a string.
                    #And then we use the strip method to remove the trailing quotes
                    table_name_to_load = operations_table_to_load_tup[-1]
                    table_name_to_load = table_name_to_load.strip("''")
                    logging.info(f'Reading data for operations {operations}')


                    operations_table_to_load_list = list(map(str, operations_table_to_load.split(', ')))
                    # logging.info(f'Reading data for operations_table_to_load_list {operations_table_to_load_list}')
                    operations_as_list = operations_table_to_load_list[:-1]                                       
                    # logging.info(f'Reading data for operations as list {operations_as_list}')
                    
                    

                    
                    


                    df_b4_processing, cursor, connection = self.read_table(table_name, operations, last_x_day = 1, limit = 0)
                    
                    

                    #Let us check the len of the dataframe before processing. If the len is zero then airflow will error out. 
                    #Hence, we would like to move to the next set of operations if the length is zero.
                    len_of_df_b4_processing = len(df_b4_processing)
                    logging.info(f'Length of dataframe before processing data is: {len_of_df_b4_processing}')
                    
                    
                    
                    if len_of_df_b4_processing == 0:
                        logging.info(f'No data to process for operations {operations}')
                        continue

                    df, col_names_list_updated, cursor, connection, col_names_tuple = self.df_flatten_explode(df_b4_processing, cursor, connection, operations_as_list)
                    
                    logging.info(f'Total length of rows of the processed dataframe from the stg_usr.amh_user_activity_prod_2  is: {len(df)}')
                    
                    logging.info(f'Total length of columns is: {len(col_names_list_updated)}')
                    logging.info(f'The column names of the processed dataframe from the {table_name} are {col_names_list_updated}')
                    logging.info(f'Table name to upload ({table_name_to_load})')
                
                    
                    logging.info('Creating database table if not already exists')
                    self.create_db_dynamically(df, table_name_to_load, cursor)
                    
                    logging.info('Database table creation completed if not already exists.')


                    #Read already existed database table so as to check if the columns are 
                    # different from the table to be loaded. This will help to check for extra column coming in afresh.
                    #The extra column coming in afresh is appended to d_diff using check_if_col_diff function.
                    df_from_dbtable,  cursor, connection, col_names = self.read_already_existed_dbtable(connection, cursor, table_name_to_load, limit = 1)
                    d_diff = self.check_if_col_diff(col_names_list_updated, col_names)
                    logging.info(f'The column difference between incoming new columns and columns already in the database table {table_name_to_load} is {d_diff}')
                    if len(d_diff) > 0:
                        #Now alter the database table by adding the new column(s).
                        self.update_dbtable_with_new_col(table_name_to_load, d_diff, cursor, col_names_list_updated, col_names)
                    
                    
                    logging.info(f'Starting to upload to database table ({table_name_to_load})')
                    unique_id = self.get_unique_id(table_name_to_load, connection, last_x_day=1)
                    logging.info('Getting unique id completed')

                    self.load_table(connection, cursor, df, col_names_tuple, table_name_to_load,unique_id)
                    logging.info('Database table upload completed!')
