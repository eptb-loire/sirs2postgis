################################################################################################################################
#Septembre2023 : création des vues géographiques (niv0) et des vues de niv1 et 2 dans les schémas sirs avec jointure vers les tables de référence
#09/02/2024 ligne 76 : un json est identifié par les caractères [{ et non uniuqment { (bug sur la table arbrevegetation de vernusson dont le json n'a pas de croche)
#20/01/2025 (Emilie) : ajout des droits de lectures sur les tables pour les comptes portant le même nom que le schéma
#20/06/2025 (Emilie): les positions réélles des objets (par opposition aux positions plaquées sur la digue) sont contenues dans le champ position ; la géométrie des vues est donc créée avec les champs positiondebut et positionfin
#21/07/2025 : dans le champ designation, concaténation de la désignation du SE et de la désignation de l'objet + concaténation de l'abrege et du libelle de la ref
#31/07/2025 : correction des liens avec les tables ref pour les vues de niveau >1 (observations notamment)
#05/08/2025 : annulation du comit du 31 juillet concernant la clause where 
#27/10/2025 : prise en compte de la géométrie multipolygone
#pour ne faire qu'une seule base : modifier temporairement auto_sirs2postgis.py pour appeler crea_view surla base souhaitée
#utiliser  : sirs_crea_view_pg_1base.bat depuis srv-adobe (192.168.1.15)
################################################################################################################################
import psycopg2
from qgis.core import QgsVectorLayer,QgsDataSourceUri,QgsRelation,QgsEditorWidgetSetup,QgsProject
from PyQt5.QtCore import *
import qgis.utils
from qgis.utils import iface
import os
import datetime


def bojour():
    print("bonjour")


def crea_view(nomdb,pgcourt):

		print(nomdb)		
		nomschema='digue_'+pgcourt+'_sirs'
		champsinutile="('author','borne_debut_aval','borne_debut_distance','borne_fin_aval','borne_fin_distance','bornedebutid','bornefinid','editedgeocoordinate','geometrymode','latitudemax','latitudemin','longitudemax','longitudemin','prdebut','prfin','systemerepid','valid')"
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
		#récupération des tables avec une géométrie
		rqtablegeoiq="select  distinct table_name from information_schema.columns where table_schema='{}' and column_name='geometry' and table_name not like  'v_%'".format(nomschema)#and table_name='desordre'
		print(nomdb)
		cur.execute(rqtablegeoiq)
		conn.commit() 
		ltablegeoiq = cur.fetchall() 
		listevueniv0=[]
		listevueniv1=[]
		listevueniv2=[]
		lrelation=[]
		listevuegeom=[]
		#création d'un dictionnaire qui va associer à chaque niveau 0 ses niveaux 1
		dict_niv0niv1={};
		#création d'un dictionnaire qui va associer à chaque niveau 1 ses niveaux 2
		dict_niv1niv2={};
		#print(ltablegeoiq)

		for tgeoiq in ltablegeoiq:
			rqlchamps="select distinct column_name from information_schema.columns  where table_schema='{}' and table_name='{}' and column_name not in{} ;".format(nomschema,tgeoiq[0],champsinutile)
			cur.execute(rqlchamps)
			lchamps=cur.fetchall()
			#on sépare selon le type de champ
			listechampjson=[]
			listechamptexte=[]
			listechamptexteavectable=[]
			listegeometrie=[]
			#la géométrie
			#il y a des fausses lignes dans sirs (point début=point fin). Il faut les détecter pour en faire des points
			#rqgeometrie="select distinct case 	when st_length(st_geomfromtext(geometry))=0 then 'point'	else 'linestring' end case  from {}.{} where geometry is not null ;".format(nomschema,tgeoiq[0])
			#27/10/2025 : prise en considération des polygones
			rqgeometrie="select distinct case when st_area(st_geomfromtext(geometry))>0 then 'multipolygon' when st_length(st_geomfromtext(geometry))>0 then 'linestring' else 'point' end case  from {}.{} where geometry is not null ;".format(nomschema,tgeoiq[0])
			cur.execute(rqgeometrie)
			lgeometrie=cur.fetchall()
			for geom in lgeometrie:
				listegeometrie.append(geom[0])
			for champsutile in lchamps:
				#si le champ est un json, il est exclu de la vue. Il sera intérrogé autrement
				#26/09/2024 : si la 1ère valeur est nulle on entre dans le cas 'None'. Il faut donc trouver la 1ère non nulle
				rqtestjson="select \"{}\" from {}.{} where {} is not null limit 1".format(champsutile[0],nomschema,tgeoiq[0],champsutile[0])
				cur.execute(rqtestjson)	
				conn.commit()
				ltestchamp=cur.fetchall()
				for testchamp in ltestchamp:
					if str(testchamp[0])!='None':
						if testchamp[0].find('[{')>-1  :
							listechampjson.append(champsutile[0])
						elif champsutile[0]!='geometry' :
							listechamptexte.append(champsutile[0])
							listechamptexteavectable.append('obj."'+champsutile[0]+'"')

			listegeometrie[:]=list(set(listegeometrie))
			#si les champs positiondebut et positionfin existent : on les transforme en géométrie et on les supprime de la liste :
			if 'positiondebut' in listechamptexte:
				#on utilise ce champ pour créer une géométrie non projetée. Ensuite, on le retire la liste des champs utilisé dans la vue
				#le champ positiondebut peut être null
				champgeompoint=' COALESCE (  st_setsrid(positiondebut::geometry, 2154),st_startpoint(st_setsrid(st_geomfromtext(obj.geometry), 2154))::geometry(Point,2154)) as geom '
				champgeomligne=' COALESCE ( st_setsrid(st_multi(st_makeline(positiondebut::geometry,positionfin::geometry)),2154), st_multi(st_setsrid(st_geomfromtext(obj.geometry), 2154))::geometry(MultiLineString,2154)) as geom '
				listechamptexteavectable.remove('obj."positiondebut"')
				listechamptexteavectable.remove('obj."positionfin"')
				listechamptexte.remove('positiondebut')
				listechamptexte.remove('positionfin')
				# clausewherepoint=' where st_length(COALESCE (  st_setsrid(obj.positiondebut::geometry, 2154),st_startpoint(st_setsrid(st_geomfromtext(obj.geometry), 2154))::geometry(Point,2154)))=0'
				# clausewhereligne=' where st_length(COALESCE ( st_setsrid(st_multi(st_makeline(obj.positiondebut::geometry,positionfin::geometry)),2154), st_multi(st_setsrid(st_geomfromtext(obj.geometry), 2154))::geometry(MultiLineString,2154)))>0'
			else :
				champgeompoint=' st_startpoint(st_setsrid(st_geomfromtext(obj.geometry), 2154))::geometry(Point,2154) AS geom '
				champgeomligne='  st_multi(st_setsrid(st_geomfromtext(obj.geometry), 2154))::geometry(MultiLineString,2154) AS geom '
				# clausewherepoint=' where  st_length(st_setsrid(st_geomfromtext(obj.geometry), 2154)) = 0::double precision'
				# clausewhereligne=' where st_length(st_setsrid(st_geomfromtext(obj.geometry), 2154)) > 0::double precision'
			#le 27/10/2025 pour le multipolygones : on utilise le champ geometry même si positiondebut/positionfin sont présent
			champgeomplgn=' st_setsrid(st_geomfromtext(obj.geometry), 2154)::geometry(MultiPolygon,2154) AS geom '

		
			champtexte=','.join(listechamptexte)


			#ajouter la nomenclature du SE dans la désignation sauf pour la table des tronçons
			#si le champ désignation existe, on lui ajoute la désignation du SE ou le nom court du SE( si la désignation du SE est vide) + on fait la jointure avec le SE
			jointurese=""
			if 'designation' in listechamptexte and 'linearid' in listechamptexte and tgeoiq[0]!='troncondigue':
				idesignation=listechamptexteavectable.index('obj."designation"')
				listechamptexteavectable[idesignation]='coalesce(systemeendiguement.designation,\''+pgcourt+'\') ||\' \'||obj.designation as designation'
				jointurese="\
					inner join digue_"+pgcourt+"_sirs.troncondigue on obj.linearid=troncondigue._id \
					inner join digue_"+pgcourt+"_sirs.digue on troncondigue.digueid=digue._id \
					inner join digue_"+pgcourt+"_sirs.systemeendiguement on digue.systemeendiguementid=systemeendiguement._id "
			#champtexteavectable a été modifié donc on refait la liste
			champtexteavectable=','.join(listechamptexteavectable)

			#récupération des champs à référence et des tables de référence
			listejointureref=''
			rqtabref="select distinct lesattributs, tableref  from digue.v_listetableref where lower (lesattributs) =any('{"+champtexte+"}') and lestables ilike'"+tgeoiq[0]+"';"
			cur.execute(rqtabref)	
			conn.commit()
			ltabref=cur.fetchall()
			for tabref in ltabref:
				champtexteavectable=champtexteavectable.replace('obj."'+tabref[0].lower()+'"',"ref{}.abrege||':'||ref{}.libelle as {}".format(tabref[0][:-2],tabref[0][:-2],tabref[0][:-2]))
				listejointureref+=" left join digue.{} as ref{} on ref{}.code::text=split_part(obj.{},':',2)".format(tabref[1],tabref[0][:-2],tabref[0][:-2],tabref[0])
			#table contenant des géométries  : champ géométrie + autres champs texte contenus dans champtexte
			for geom in listegeometrie:
				#sur les vues successives, risque d'avoir plusieurs fois gid ; si on compte le nombre de 'v_', cela nous donne le nombre d'increment
				nbvu='gid'+str('v_{}_{}'.format(tgeoiq[0],geom[0]).count('v_'))
				#attention aux fausses lignes qui sont des points. Il faut constuire la requête sur le critère de la longueur de la géométrie
				if geom=='point':
					# 2025-04-07 : pour les désordres, il faut créer la géométrie à partir de position début/position fin car sinon c'est en mode "plaqué"
					rqcreaview= "\
					drop view if exists "+nomschema+".v_"+tgeoiq[0]+"_p ;\
					create or replace view "+nomschema+".v_"+tgeoiq[0]+"_"+geom[0]+" as (select row_number() over() "+nbvu+","+champtexteavectable+", "+champgeompoint+" from "+nomschema+"."+tgeoiq[0]+" as obj "+listejointureref+jointurese+" where st_length(st_setsrid(st_geomfromtext(obj.geometry), 2154))=0);\
					comment ON view  "+nomschema+".v_"+tgeoiq[0]+"_"+geom[0]+" is 'Créée le "+str(datetime.date.today())+"';\
					grant select on table "+nomschema+".v_"+tgeoiq[0]+"_"+geom[0]+" to \"EPLoire_Consult\";\
					grant select on table "+nomschema+".v_"+tgeoiq[0]+"_"+geom[0]+" to "+nomschema+";\
					"
					#print(rqcreaview)
				#le 29/05/2024 : il existe des multilinestring dans couchdb. On passe donc tout en MultiLineString
				# 2025-04-07 : pour les désordres, il faut créer la géométrie à partir de position début/position fin car sinon c'est en mode "plaqué" ; ajout du code corresondant à la condistion linestring+desordre
				if geom=='linestring':
					rqcreaview= "\
					drop view if exists "+nomschema+".v_"+tgeoiq[0]+"_l ;\
					create or replace view "+nomschema+".v_"+tgeoiq[0]+"_"+geom[0]+" as (select row_number() over() "+nbvu+","+champtexteavectable+","+champgeomligne+" from "+nomschema+"."+tgeoiq[0]+" as obj "+listejointureref+jointurese+" where st_length(st_setsrid(st_geomfromtext(obj.geometry), 2154))>0 );\
					comment ON view  "+nomschema+".v_"+tgeoiq[0]+"_"+geom[0]+" is 'Créée le "+str(datetime.date.today())+"';\
					grant select on table "+nomschema+".v_"+tgeoiq[0]+"_"+geom[0]+" to \"EPLoire_Consult\";\
					grant select on table "+nomschema+".v_"+tgeoiq[0]+"_"+geom[0]+" to "+nomschema+";\
					"
				#27/10/2025 : prise en compte des polygones
				if geom=='multipolygon':
					# print('table et champs : ')
					# print(tgeoiq[0])#bornedigue
					# print(listechamptexte)
					rqcreaview= "\
					drop view if exists "+nomschema+".v_"+tgeoiq[0]+"_m ;\
					create or replace view "+nomschema+".v_"+tgeoiq[0]+"_"+geom[0]+" as (select row_number() over() "+nbvu+","+champtexteavectable+","+champgeomplgn+" from "+nomschema+"."+tgeoiq[0]+" as obj "+listejointureref+jointurese+" where  obj.geometry ~~* 'multipolygon%'::text );\
					comment ON view  "+nomschema+".v_"+tgeoiq[0]+"_"+geom[0]+" is 'Créée le "+str(datetime.date.today())+"';\
					grant select on table "+nomschema+".v_"+tgeoiq[0]+"_"+geom[0]+" to \"EPLoire_Consult\";\
					grant select on table "+nomschema+".v_"+tgeoiq[0]+"_"+geom[0]+" to "+nomschema+";\
					"					
				cur.execute(rqcreaview)
				conn.commit()
				listevueniv0.append("v_{}_{}".format(tgeoiq[0],geom[0]))
			#création des tables associées de 1er niveau. Par exemple, les observations ou les photos accrochées directement aux objets : v_desordreobservations, 
			if len(listechampjson)>0:
				for cjson in listechampjson:
					#liste de toutes les clés possible pour le champ de type json en cours
					rqlistechampjsonniv1="\
						with t as(\
						select \
						_id,\
						arr.position,\
						arr.item_object::json c \
						from {}.{},jsonb_array_elements({}::jsonb) with ordinality arr(item_object, position)\
						)\
						select \
						distinct (key) \
						from t,json_each(t.c);".format(nomschema,tgeoiq[0],cjson)
					cur.execute(rqlistechampjsonniv1)
					conn.commit
					resultlistechampjsonniv1=cur.fetchall()
					#création d'un tableau contenant les noms des vues de niv1
					listevueniv1.append('v_{}'.format(tgeoiq[0]+cjson))
					rqvuegeoiq="select  distinct table_name from information_schema.columns where table_schema='{}' and table_name ilike 'v_{}__'".format(nomschema,tgeoiq[0])
					cur.execute(rqvuegeoiq)
					conn.commit
					resultlistevuegeoiq=cur.fetchall()
					for g in resultlistevuegeoiq:
						rqveriftablevide="select count(*) from {}.{}".format(nomschema,g[0])
						cur.execute(rqveriftablevide)
						conn.commit
						resultveriftablevide=cur.fetchall()
						if resultveriftablevide[0][0]>0:
							lrelation.append('r1-'+g[0]+'-v_'+tgeoiq[0]+cjson)
					rqcreaviewniv1="drop view if exists  {}.v_{} cascade;\
						create view {}.v_{} as(\
						with t as(\
						select \
						_id,\
						arr.position,\
						arr.item_object::json key\
						from {}.{},jsonb_array_elements({}::jsonb) with ordinality arr(item_object, position)\
						)\
						select  row_number() over() ::text gid,\
						_id".format(nomschema,tgeoiq[0]+cjson,nomschema,tgeoiq[0]+cjson,nomschema,tgeoiq[0],cjson)
					lclejsonniv1=[]

					for l in resultlistechampjsonniv1:
						if l[0]!='_id':
							rqcreaviewniv1=rqcreaviewniv1+",key->>'{}' as \"{}\"".format(l[0],l[0])
							lclejsonniv1.append(l[0])
					#liste des clés du json me permettant de chercher si ce sont des champ de réfénence
					clejsonniv1=','.join(lclejsonniv1)
					listejointurerefniv1=''
					#dans le critère lestables ilike on supprime le dernier caractère car il peut y avoir un s dans le champs json et pas dans la table originale
					#31/07/2025 : certaines tables d observations s'appellent abstractObersvation
					rqtabref="select distinct on  (lesattributs, tableref) lesattributs, tableref,tableref||row_number() over (partition by tableref order by lesattributs)::text rk  from digue.v_listetableref where lower (lesattributs) =any('{"+clejsonniv1+"}') and lestables ilike'%"+cjson[:-1]+"%';"
					cur.execute(rqtabref)	
					conn.commit()
					ltabref=cur.fetchall()
					for tabref in ltabref:
						rqcreaviewniv1=rqcreaviewniv1.replace("key->>'"+tabref[0].lower()+"' as \""+tabref[0].lower()+"\"",tabref[2]+".libelle as "+tabref[0][:-2])
						listejointurerefniv1=listejointurerefniv1+" left join digue."+tabref[1]+" as "+tabref[2]+" on "+tabref[2]+".code::text=split_part(key->>'"+tabref[0].lower()+"',':',2)"
					rqcreaviewniv1=rqcreaviewniv1+" from t "
					print(rqcreaviewniv1+listejointurerefniv1)
					cur.execute(rqcreaviewniv1+listejointurerefniv1+");grant select on table {}.v_{} to \"EPLoire_Consult\",{};".format(nomschema,tgeoiq[0]+cjson,nomschema))
					
					conn.commit()
		for vue1 in listevueniv1:
			#liste des champs utiles dans les vues de niveau 1 (dans  v_desordreobservations par exemple)
			rqlchampsniv1="select distinct column_name from information_schema.columns  where table_schema='{}' and table_name='{}' and column_name not in{} ;".format(nomschema,vue1,champsinutile)
			cur.execute(rqlchampsniv1)
			conn.commit()
			listechamputileniv1=cur.fetchall()
			listechampjsonniv1=[]
			listechamptexteniv1=[]

			for champutileniv1 in listechamputileniv1:
				rqtestjsonniv1="select \"{}\" from {}.{}  where \"{}\" is not null limit 1".format(champutileniv1[0],nomschema,vue1,champutileniv1[0])
				cur.execute(rqtestjsonniv1)	
				conn.commit()
				ltestchampniv1=cur.fetchall()
				#rséparation des champs json et des champs texte
				for testchampniv1 in ltestchampniv1:
					if str(testchampniv1[0])!='None':
						if testchampniv1[0].find('{')>-1  :
							listechampjsonniv1.append(champutileniv1[0])
						else:
							listechamptexteniv1.append(champutileniv1[0])
			

			if len(listechampjsonniv1)>0:
				for cjsonniv1 in listechampjsonniv1:
					#liste de toutes les clés possible pour le champ de type json en cours (donnera une table de niveau 2, ex v_desordreobservationsphotos)
					rqlistechampjsonniv2="\
						with t as(\
						select \
						_id,\
						arr.position,\
						arr.item_object::json c \
						from {}.{},jsonb_array_elements({}::jsonb) with ordinality arr(item_object, position)\
						)\
						select \
						distinct (key) \
						from t,json_each(t.c) where key not in {};".format(nomschema,vue1,cjsonniv1,champsinutile)
					cur.execute(rqlistechampjsonniv2)
					conn.commit
					resultlistechampjsonniv2=cur.fetchall()
					#à partir des champs des jsons, création des vues de niveau 2
					rqcreaviewniv2="drop view if exists  {}.{} cascade;\
						create view {}.{} as(\
						with t as(\
						select\
						id as id{},\
						arr.position,\
						arr.item_object::json key\
						from {}.{},jsonb_array_elements({}::jsonb) with ordinality arr(item_object, position)\
						)\
						select  row_number() over() gid,\
						id{} ".format(nomschema,vue1+cjsonniv1,nomschema,vue1+cjsonniv1,vue1,nomschema,vue1,cjsonniv1,vue1)
					listevueniv2.append(vue1+cjsonniv1)
					lrelation.append('r2-'+vue1+'-'+vue1+cjsonniv1);
					for l2 in resultlistechampjsonniv2:
						if l2!='_id':
							rqcreaviewniv2=rqcreaviewniv2+",key->>'{}' as \"{}\"".format(l2[0],l2[0])
					
					rqcreaviewniv2=rqcreaviewniv2+" from t);\
					grant select on table {}.{} to \"EPLoire_Consult\";\
					grant select on table {}.{} to {};".format(nomschema,vue1+cjsonniv1,nomschema,vue1+cjsonniv1,nomschema)
					#print(rqcreaviewniv2)
					cur.execute(rqcreaviewniv2)
					conn.commit()
		#lrelation contient la liste des relations entre table : r_v_vuemere-r_v_vuefille. Le niveau de la relation est indiquée avec r1 et r2 (dans r1 la table mère est une table géographique)
		#dans les relations r1, le champ commune est _id
		#dans le relation r2 le champ commune est tablemere.id=v_vuefille.idv_vuemere ; ex : v_desordreobservations.id=v_desordreobservationsphotos.idv_desordreobservations
