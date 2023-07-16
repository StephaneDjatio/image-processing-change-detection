
from sqlalchemy import create_engine
import geopandas as gpd
import psycopg2



def push_table(vector, user, password, host, port, database, table_name) : 

    user = user
    password = password
    host = host
    port = port
    database = database
    
    conn = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(conn)
    
    #Read shapefile using GeoPandas
    gdf = gpd.read_file(vector)
    
    #Import shapefile to databse
    gdf.to_postgis(name=table_name , con=engine)
    
    print("The table has been succesfully uploaded.")



def drop_table(user, password, host, port, database, table_name) :

    conn=psycopg2.connect(
        user = user,
        password = password,
        host = host,
        port = port,
        database = database
    )
    
    
    # Creating a cursor object using the cursor() 
    # method
    cursor = conn.cursor()
    
    # drop table accounts
    sql  = '''DROP TABLE  '''
    sql += table_name
    
    # Executing the 
    try : 
        cursor.execute(sql)

        # Commit your changes in the database
        conn.commit()
        
        # Closing the connection
        conn.close()
        print("Table dropped !")
    except : 
        print("The table you wanted to delete does not exist yet.")