################################################################################################################################
#avril 2025 : à lancer dans qgis (macro de projet) pour ajouter les couches du SE concerné + créer les relations entre les tables
#nom du se contenu dans le nom du projet qgis
#juin 2025 : connection à la base de données via l'entrée du DBManager (distribuée sur les PC avec QDT)
################################################################################################################################
import psycopg2
from qgis.core import QgsVectorLayer,QgsDataSourceUri,QgsRelation,QgsEditorWidgetSetup,QgsProject
from PyQt5.QtCore import *
import qgis.utils
from qgis.utils import iface

def leprojet():
		project = QgsProject.instance()
		nomcomplet=project.fileName()
		#print(nomcomplet.rfind('/'))
		nomprojet=nomcomplet[nomcomplet.rfind('/')+1:]
		nomse=nomprojet[nomprojet.index('_')+1:-4]
		nomschema='digue_{}_sirs'.format(nomse)
		#################################################################QGIS
		##################################################################
		###########récup des paramètres de configurations enregistrés pour la connection à PG
		settings = QSettings()
		settings.beginGroup( '/PostgreSQL/connections/EPLoire_Consult' )
		db=settings.value("database")
		username=settings.value("username")
		host=settings.value("host")
		pwd=settings.value("password")
		port=settings.value("port")
		if db is not None : 
			print("utilisation de l entrée EPLoire_Consult du DBManager")
			conn = psycopg2.connect(database=db, 
	                             user=username,
	                             host=host,
	                             password=pwd,
	                             port=port )
			cur = conn.cursor()

			#Ajout des couches
			uri = QgsDataSourceUri()
			uri.setConnection(host, port, db,username,pwd)
			#récupère la liste des vues à ajouter et classement en liste niveau0, niveau1
			rqlistetable="\
				select \
				distinct table_name ,\
				case\
					when table_name like '%_p' or table_name like '%_l' then 'niv0'\
					when string_agg( column_name,',') like '%\_id%' then 'niv1'\
					else 'niv2'\
				end as niveau\
				from information_schema.columns\
				where table_schema='"+nomschema+"' and table_name like 'v%'\
				group by  table_name\
				order by niveau, table_name\
				"
			cur.execute(rqlistetable)
			conn.commit() 
			rows = cur.fetchall()  #parcourt les resultats du cur pour faire afficher ici la premiere colonne de la table 
			listevueniv0=[]
			listevueniv1=[]
			listevueniv2=[]
			for r in rows:
				if r[1]=='niv0':
					listevueniv0.append(r[0])
				elif r[1]=='niv1':
					listevueniv1.append(r[0])		 
				elif r[1]=='niv2':
					listevueniv2.append(r[0])

			for g in listevueniv0:
				uri.setDataSource(nomschema, g, "geom")
				uri.setKeyColumn("_id")
				QgsProject.instance().addMapLayer(QgsVectorLayer(uri.uri(False), g, "postgres"))

			for v in (listevueniv1+listevueniv2):
				uri.setDataSource(nomschema, v, None)
				uri.setKeyColumn("gid")
				QgsProject.instance().addMapLayer(QgsVectorLayer(uri.uri(False), v, "postgres"))

			#il faut refaire les relations
			lrelation=[]
			rqliaison="\
				with recursive t(table_name, champ, niveau, nomobjet,gid) as\
				(\
				select * from \
						(select \
							distinct table_name ,\
							string_agg(column_name,',') champ,\
							case \
								when table_name like '%_p' or table_name like '%_l' then 'niv0' \
								when string_agg( column_name,',') like '%\_id%' then 'niv1' \
								else 'niv2' \
							end as niveau,\
							case \
								when table_name like '%_p' or table_name like '%_l' \
								then substring(table_name from position ('_'in table_name)+1 for (length(table_name)-4)) \
								else '' \
							end as nomobjet, \
						 row_number() over()gid \
							from information_schema.columns \
							where table_schema='"+nomschema+"' and table_name like 'v%' \
							group by  table_name \
							order by table_name, niveau) \
					as depart \
					where nomobjet='bornedigue' \
					union \
				select \
					t1.table_name,t1.champ,\
					t1.niveau,\
					case \
						when t1.niveau='niv0' then t1.nomobjet \
						else t.nomobjet \
					end as nomobjet,\
					t1.gid \
					from (select \
							distinct table_name ,\
							string_agg(column_name,',') champ, \
							case \
								when table_name like '%_p' or table_name like '%_l' then 'niv0' \
								when string_agg( column_name,',') like '%\_id%' then 'niv1' \
								else 'niv2' \
							end as niveau,\
							case \
								when table_name like '%_p' or table_name like '%_l' \
								then substring(table_name from position ('_'in table_name)+1 for (length(table_name)-4)) \
								else '' \
							end as nomobjet,\
							row_number() over()gid \
							from information_schema.columns \
							where table_schema='"+nomschema+"' and table_name like 'v%' \
							group by  table_name \
							order by table_name, niveau) \
					as   t1 inner join t on t.gid=t1.gid-1\
				),\
				niv0 as(\
				select table_name, niveau, nomobjet  from t where niveau='niv0')\
				,\
				niv1 as(\
				select table_name, niveau, nomobjet  from t where niveau='niv1')\
				,\
				niv2 as(\
				select table_name, niveau, nomobjet  from t where niveau='niv2')	\
				select \
					'r1-'||niv0.table_name||'-'||niv1.table_name\
					from niv0 inner join niv1 on niv0.nomobjet=niv1.nomobjet \
				UNION	\
				select\
					'r2-'||niv1.table_name||'-'||niv2.table_name\
					from niv1 inner join niv2 on niv1.nomobjet=niv2.nomobjet \
				"	
			cur.execute(rqliaison)
			conn.commit() 
			rows = cur.fetchall()  
			for r in rows:
				lrelation.append(r[0]);
			#creation de dictionnaire pour associer les tables mères/filles et leurs identifiants

			rel = QgsRelation()
			dict_rmere={}
			dict_rfille={}
			dict_ridmere={}
			dict_ridfille={}
			layers = QgsProject.instance().mapLayers()
			for l in layers:
			    for r in lrelation:
			        nivr=r.split('-')[0]
			        mere=r.split('-')[1]
			        fille=r.split('-')[2]
			        if layers[l].name()==mere:
			            dict_rmere[r]=l
			            if nivr=='r1':
			                dict_ridmere[r]='_id'
			            else :
			                dict_ridmere[r]='id'
			        if layers[l].name()==fille:
			            dict_rfille[r]=l
			            if nivr=='r1':
			                dict_ridfille[r]='_id'
			            else :
			                dict_ridfille[r]='id'+mere
			for r in lrelation:
				rel=QgsRelation()
				rel.setReferencingLayer( dict_rfille[r])
				rel.setReferencedLayer( dict_rmere[r] )
				rel.addFieldPair( dict_ridfille[r],dict_ridmere[r] )
				rel.setId( r )
				rel.setName(r)
				QgsProject.instance().relationManager().addRelation( rel )

			#♣configuration des champs dans le formulaire (affichage des photos, masquage des indésirable)
			widgetchemin = QgsEditorWidgetSetup(
			    'ExternalResource', 
			    {
			        'FileWidget': True,
			        'DocumentViewer': 1,
			        'RelativeStorage': 0,
			        'StorageMode': 0,
			        'DocumentViewerHeight': 500,
			        'FileWidgetButton': True,
			        'DocumentViewerWidth': 500,
			        'FileWidgetFilter': ''
			    })
			widgetcache = QgsEditorWidgetSetup(
			    'Hidden',{})
			for l in layers.values():
			    indexchemin=l.fields().indexFromName('chemin')
			    indexclasse=l.fields().indexFromName('@class')
			    indexvalid=l.fields().indexFromName('valid')
			    indexphoto=l.fields().indexFromName('photos')
			    indexurgence=l.fields().indexFromName('urgence')
			    if indexchemin>-1:
			        l.setEditorWidgetSetup(indexchemin, widgetchemin)
			    if indexclasse>-1:
			        l.setEditorWidgetSetup(indexclasse, widgetcache)
			    if indexvalid>-1:
			        l.setEditorWidgetSetup(indexvalid, widgetcache)
			    if indexphoto>-1:
			        l.setEditorWidgetSetup(indexphoto, widgetcache)
			    if indexurgence>-1:
			        l.setDisplayExpression('"date"+\' : \'+"urgence"')
		else : 			      
			    print("pas de base enregistrée dans db manager")

