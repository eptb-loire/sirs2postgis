# sirs2postgis
 Envoi des données de SIRS DIGUES dans PostGis
Scripts créés par l'Etablissement public Loire (Emilie BIGORNE).
 

Envoi des données de CouchDB dans PostGresql :exécution automatique toutes les nuits de auto_sirs2postgis : 
	- sirs2postgis(nom base couchdb, nom court PG) : envoi des données de CouchDB dans PostgreSql. 1 base couchDB correspond à un schéma PG. Géométries au format texte. Dans PostgreSQL la table sirs.couchdb_pg fait le lien entre le nom de la base CouchDB et le nom du schéma PG
	- f_creation_tabl_xml.sql)  récupation dans les xml de Sirs Digues de la structure des objets (nom de la tables et champs). Les xml doivent être déposés dans le répertoire data de PostgreSql (ex : C:\Program Files\PostgreSQL\14\data). Pour récupérer les xml : https://github.com/Geomatys/SIRS-Digues-V2 / nom_du_plugin/model/www.ecore
	- sirs_crea_view_pg(nom base couchdb, nom court PG) : création des vues avec géométrie dans dans PG  ; pour chaque type d'objet SIRS, 2 vues PG :une pour les lignes, ou pour les points.
Identifiants/mots de passe enregistrés dans les variables d'environnement du serveur sur lequel tourne le script


Affichage des données dans QGis : exécution via un macro d'un script dans QGis permettant d'afficher les vues et de générer les relations entre objets (relation 1 à n entre un objet et ses obervations et entre une observation et ses photos) : 
	- sirs_crea_qgs : le nom du projet qgis permet de récupérer le nom du schéma pg


Pour récupérer les xml : https://github.com/Geomatys/SIRS-Digues-V2 / nom_du_plugin/model/www.ecore



