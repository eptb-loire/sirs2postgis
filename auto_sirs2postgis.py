##l'objectif ici est de faire la liste des champs presente dans la table sirs.couchdb_pg et de les utiliser
## comme argument dans la fn sirs2postgis 

# python S:\Dossiers_Agents\EBigorne\Postgres\requetes\auto_sirs2postgis.py
import numpy as np
import datetime
import psycopg2
from sirs2postgis import sirs2postgis
from sirs_crea_view_pg import crea_view
import os

#Connexion a Pg
conn = psycopg2.connect(database=os.getenv('PGDATABASE'), 
                             user=os.getenv('PGUSER'),
                             host=os.getenv('PGHOST'),
                             password=os.getenv('PGPASSWORD'),
                             port=os.getenv('PGPORT')) 
cur = conn.cursor()
#recup des valeurs de la colonne nomdb 

db = "SELECT  distinct couchdb_pg.nomdb, couchdb_pg.nompg  from sirs.couchdb_pg ;"
cur.execute(db)
conn.commit() 

rows = cur.fetchall()  #parcourt les resultats du cur pour faire afficher ici la premiere colonne de la table 
l2=[] #creation liste vide
for r in rows:
    sirs2postgis(r[0],r[1])
    crea_view(r[0],r[1])








	