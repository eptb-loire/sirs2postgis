# sirs2postgis
 Envoi des données de SIRS DIGUES dans PostGis
Scripts créés par l'Etablissement public Loire (Emilie BIGORNE).
 
Structuration : 
	- 1 base couchDB (SIRS) correspond à 1 schéma PG
	- dans PG une table fait le lien entre les noms des base CouchDB et les noms des schémas PG (sirs.couchdb_pg)
	- Les différents types d'objet SIRS sont stockés dans des tables sous PG. La géométrie y est stockée au format text. Les objets enfants (comme les observations) sont stockés en json dans un champ de la table parent
	- les objets contenant une information géométrique sont transformés en vue avec un vrai champ de géométrie. Une table peut donner lieu à 1, 2 ou 3 vues pour séparer les types de géométrie (point, ligne, polygone).
	- Les objets enfants sont stockés en vue sans géométrie

Envoi des données de CouchDB dans PostGresql :exécution automatique toutes les nuits via auto_sirs2postgis de : 
	- sirs2postgis(nom base couchdb, nom court PG) : envoi des données de CouchDB dans PostgreSql. Géométries au format texte. 
	- f_creation_tabl_xml.sql)  récupation dans les xml de Sirs Digues de la structure des objets (nom de la tables et champs). Les xml doivent être déposés dans le répertoire data de PostgreSql (ex : C:\Program Files\PostgreSQL\14\data). Pour récupérer les xml : https://github.com/Geomatys/SIRS-Digues-V2 / nom_du_plugin/model/www.ecore
	- sirs_crea_view_pg(nom base couchdb, nom court PG) : création des vues avec géométrie dans dans PG .
Identifiants/mots de passe enregistrés dans les variables d'environnement du serveur sur lequel tourne le script


Affichage des données dans QGis : exécution via une macro d'un script dans QGis permettant d'afficher les vues et de générer les relations entre objets (relation 1 à n entre un objet et ses obervations et entre une observation et ses photos) : 
	- sirs_crea_qgs : le nom du projet qgis permet de récupérer le nom du schéma pg


Pour récupérer les xml : https://github.com/Geomatys/SIRS-Digues-V2 / nom_du_plugin/model/www.ecore



