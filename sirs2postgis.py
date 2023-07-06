###################################################################################################
#injecte les donnees de SIRS dans PG dans le schema désigné. Si schema et tables existent, ils sont vidés
#python S:\Dossiers_Agents\EBigorne\sirs2postgis\sirs2postgis.py
#https://github.com/Geomatys/SIRS-Digues-V2/blob/master/sirs-core/model/sirs.ecore -->
####################################################################################################
import couchdb
import json
import numpy as np
import datetime
from lxml import etree
import xml.etree.ElementTree as ET
import psycopg2


def sirs2postgis():    
    # Saisie serveur sirs
    print('saisir l''adresse du serveur sur lequel est installé SIRS')
    hotesirs=input()
    #Saisie serveur PG
    print('saisir l''adresse du serveur sur lequel est installé PostgreSql')
    hotepg=input()
    #saisie nom de la base CouchDB
    print ('saisir nom de la base couchDB')
    nomdbsirs=input()
    #saisie nom de la base pg
    print('saisir le nom de la base PostgreSql')
    nomdbpg=input()
    #saisie nonm du schéma PG
    print('saisir le schéma PostgreSql')
    schemapg=input()
    #saisie utilisateur PG
    print ('Utilisateur PG')
    userpg=input()
    #Saisie pwd pg
    print ('pwd utilisateur PG')
    pwpg=input()

    #connexion à CouchDB
    couch = couchdb.Server('http://geouser:geopw@{}:5984/'.format(hotesirs))
    db=couch['{}'.format(nomdbsirs)]

    #Connexion a Pg
    conn = psycopg2.connect(database=nomdbpg, 
                             user=userpg,
                             host=hotepg,
                             password=pwpg,
                             port="5432") 
    cur = conn.cursor()

    #creer le schema s'il n'existe pas 
    design = 'sirs' #designation pour indiquer dans le nom de la table que les donnees proviennent de sirs
    addschema="CREATE SCHEMA if NOT EXISTS {} ;".format(schemapg)    #creer le schema
    cur.execute(addschema)

    #faire la liste des tables du schema qui existe. Ces tables pouvant avoir des dépendances, on ne les supprime pas si elles existent, on les vide
    d = "SELECT distinct table_name from information_schema.columns where table_schema = '{}' and is_updatable= 'YES' ;".format(schemapg)
    cur.execute(d)
    conn.commit() 
    rows = cur.fetchall()  #parcourt les resultats du cur pour faire afficher ici la premiere colonne de la table 
    l2=[] #liste des tables

    for x in rows:
        l1 = ''.join(x) 
        l2.append(l1)
    conn.commit() 

     #truncate sur les tables existantes
    for x in l2:    
        montruncate = "TRUNCATE TABLE {}.{};".format(schemapg,x)
        cur.execute(montruncate)
        conn.commit()
    i=0
    #récupération des objets dans CouchDB
    for item in db:
        i=i+1
        monid=db[item] 
        maclasse=monid.get('@class') #récupération des enregistrements contenant la valeur @class <==> objets dans SIRS
        if maclasse:
            if maclasse[19:22]!='Ref': #on ne récupère pas les références

                if maclasse[19:].lower() not in l2:
                    #création des tables
                    l2.append(maclasse[19:])#on ajoute à la liste les tables que l'on n'a pas encore
                    #ici, adapter le nom du schema dans lequel est rangé la fonction
                    monappelfn = "select * from sirs.f_creation_tabl_xml('{}','{}')".format(schemapg,maclasse[19:])
                    cur.execute(monappelfn)
                    conn.commit()
                    ligne_fn=cur.fetchall()
                    l_fn = []
                    for x in ligne_fn: 
                        la = ''.join(x)
                        l_fn.append(la)
                    conn.commit()

                    for x in l_fn: 
                      mesrequetes = x
                    cur.execute(mesrequetes)
                    conn.commit()
                    

          #insertion des donnees  manquantes de la table
                moninsert = """insert into {}.{} select * from json_populate_record(NULL::{}.{},%s)""".format(schemapg,maclasse[19:],schemapg,maclasse[19:])
                cur.execute(moninsert,(json.dumps(monid).lower(),))
                conn.commit()
                moncomment ="""comment on table {}.{} is 'Importe de la base sirs {} le {}'""".format(schemapg,maclasse[19:],nomdbsirs,str(datetime.date.today()))
                cur.execute(moncomment)
                conn.commit()

sirs2postgis()