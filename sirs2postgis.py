###################################################################################################
#Contributeur : Salomé MAYER, Emilie BIGORNE
#injecte les donnees de SIRS dans PG dans le schema digue_xxx_sirs. Si schema et tables existent, elles sont videes (suppression impossible en raison des dépendances)
#lire un json : https://www.percona.com/blog/storing-and-using-json-within-postgresql-part-two
#faire la difference entre deux listes : https://www.delftstack.com/fr/howto/python/difference-between-two-lists-python/ 
#<!-- chemin d'acces du github
#https://github.com/Geomatys/SIRS-Digues-V2/blob/master/sirs-core/model/sirs.ecore -->
#Janvier 2025 : ajout des droits pour le groupe d'utilistateur portant le même nom que la base

####################################################################################################
import couchdb
import json
import numpy as np
import datetime
from lxml import etree
import xml.etree.ElementTree as ET
import psycopg2
import os

def sirs2postgis(nomdb,pgcourt):    
    print(nomdb)
    now= datetime.datetime.now()
    # #dialogue utilisateur
    #hote=input()
    hote='192.168.1.7'
    #print ('saisir nom BD de couchDB')
    #nomdb=input()
    #nomdb='loire_vernusson'
    #print('saisir le nom du systeme , ex: vernusson')
    pg='digue_'+pgcourt+'_sirs'
    #pg='digue_vernusson_sirs'
    couch = couchdb.Server('http://geouser:geopw@{}:5984/'.format(hote))
    db=couch['{}'.format(nomdb)]
    #print ('Utilisateur PG')
    userpg='Emilie'
    #userpg=input()
    #print ('pwd utilisateur PG')
    pwpg='***'
    #pwpg=input()


    # #Connexion a Pg

    # conn = psycopg2.connect(database="EPLoire", 
    #                          user=userpg,
    #                          host="192.168.1.7",
    #                          password=pwpg,
    #                          port="5432") 
    # cur = conn.cursor()
    #Connexion a Pg
    dbpg=os.getenv("PGDATABASE")
    userpg=os.getenv("PGUSER")
    hostpg=os.getenv("PGHOST")
    pwpg=os.getenv("PGPASSWORD")
    portpg=os.getenv("PGPORT")


    conn = psycopg2.connect(database=os.getenv('PGDATABASE'), 
                                 user=os.getenv('PGUSER'),
                                 host=os.getenv('PGHOST'),
                                 password=os.getenv('PGPASSWORD'),
                                 port=os.getenv('PGPORT')) 
    cur = conn.cursor()
    #creer le schema s'il n'existe pas 
    design = 'sirs' #designation pour indiquer dans le nom de la table que les donnees proviennent de sirs
    addschema="CREATE SCHEMA if NOT EXISTS {} ;".format(pg)    #creer le schema
    cur.execute(addschema)
    # donner le droit
    grantallschema = "grant all on schema {} to \"Emilie\", apprenti_carto".format(pg)
    cur.execute (grantallschema)
    granselectschema = "grant usage on schema {} to spi_consult, \"SAGE\",{}".format(pg,pg)
    cur.execute (granselectschema)

    #faire la liste des table du schema qui existe 
    #estce que l'objet est dans la liste si oui je le vide si non je le cree 
    d = "SELECT distinct table_name from information_schema.columns where table_schema = '{}' and is_updatable= 'YES' ;".format(pg)
    cur.execute(d)
    conn.commit() 


    rows = cur.fetchall()  #parcourt les resultats du cur pour faire afficher ici la premiere colonne de la table 
    l2=[] #creation liste vide

    for x in rows:
        l1 = ''.join(x) # ligne62/63, on sors de rows les donnees et elles sont converties en str 
        l2.append(l1) #rempli la liste l2 avec les donnees str de l1
    conn.commit() 


     #etape du truncate afin de vider les tables qui existe dans l2 (postgre)

    for x in l2:    
        

        montruncate = "TRUNCATE TABLE {}.{};".format(pg,x)
        cur.execute(montruncate)
        conn.commit()
    i=0

    for item in db:
        i=i+1
        monid=db[item] 

        maclasse=monid.get('@class') #on obtient toutes les classes presentes dans couchdb 

        if maclasse:
            #print(maclasse)
            if maclasse[19:22]!='Ref':

                if maclasse[19:].lower() not in l2:#une table par type d'objet  , 
                    l2.append(maclasse[19:].lower())
                   #marequete="create table if not exists {}.{}( _id text, ".format(pg,maclasse[19:])

                    ##debut de l utilisation de la fonciton SQL qui recupere les tables dans le xml et les creer
                    monappelfn = "select * from sirs.f_creation_tabl_xml('{}','{}')".format(pg,maclasse[19:])
                    cur.execute(monappelfn)
                    conn.commit()
                          #on recupere les resultats de l'appel de fonction 
                    ligne_fn=cur.fetchall()
                    if len(ligne_fn):
                        l_fn = []
                        for x in ligne_fn: 
                            la = ''.join(x)
                            l_fn.append(la)

                        conn.commit()

                        for x in l_fn: 
                          mesrequetes = x
                        cur.execute(mesrequetes)
                        conn.commit()
                    
                if '.' not in maclasse[19:] :
                    grantselecttable = "grant select on table {}.{} to spi_consult, \"SAGE\",\"EPLoire_Consult\",{}".format(pg,maclasse[19:],pg)
                    #print(grantselecttable)
                    cur.execute (grantselecttable)
                    conn.commit()

          #insertion des donnees  manquantes de la table
                    moninsert = """insert into {}.{} select * from json_populate_record(NULL::{}.{},%s)""".format(pg,maclasse[19:],pg,maclasse[19:])
                    cur.execute(moninsert,(json.dumps(monid).lower(),))
                    conn.commit()
                    moncomment ="""comment on table {}.{} is 'Importe de la base sirs {} le {}'""".format(pg,maclasse[19:],nomdb,str(datetime.date.today()))
                    cur.execute(moncomment)
                    conn.commit()


    d = "Insert into  sirs.couchdb_pg(nomdb, nompg) values('{}','{}') on conflict on constraint couchdb_pg_nomdb_nompg_key do nothing;".format(nomdb,pgcourt)
    cur.execute(d)
    conn.commit() 
    print(l2)
    os.chdir("\\\\SRV-fic01\\Cartographie\\sirs_svg")
    monRepertoire ="\\\\SRV-fic01\\Cartographie\\sirs_svg"
    nomfichier="sirs2postgis{}.txt".format(now.strftime("%Y-%m-%d"))
    listefichiers = open(nomfichier,"a")
    listefichiers.write(now.strftime("%Y-%m-%d %H:%M"))
    listefichiers.write(nomdb + "\\n")
    listefichiers.write(' '.join(l2) + "\\n")
    # print('script execute du systeme : ',pg)
#sirs2postgis('oudan_roanneoudan','roanneoudan')

