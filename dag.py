import requests
import json
import psycopg2



def getData():
    r = requests.get('https://randomuser.me/api')
    userData = json.loads(r.content.decode('utf-8'))['results'][0]
    gender = userData['gender']
    userName = userData['name']
    name = '{} {} {}'.format(userName['title'],  userName['first'], userName['last']) 
    userLocation = userData['location']
    street = userLocation['street']['name']
    city = userLocation['city']
    state = userLocation['state']
    country = userLocation['country']
    email = userData['email']
    #print(gender, '  ',name, '  ', street,'  ' , city,'  ' , state,'  ' , country,'  ', email)
    return (name, gender, street , city, state, country, email)



#getData()


data = getData()

con=psycopg2.connect(dbname="mosaico", host="10.20.3.63",  
    port="5439", user="galarcon", password="gA072021*!a")
cur = con.cursor()



cur.execute("INSERT INTO sandbox.primer_dag_gonzalo (name, gender , street , city, state, country, email) VALUES \
    (%s, %s, %s, %s, %s, %s, %s) ;", data )

con.commit()

cur.execute("SELECT * FROM sandbox.primer_dag_gonzalo;")
rows = cur.fetchall()
cur.close() 
con.close()

for row in rows:
    print(row)


# extraer datos de la API -> Insertar datos en redshift