# sirs2postgis
 Envoi des données de SIRS DIGUES dans PostGis
Scripts créés par l'Etablissement public Loire en juillet 2022.
 
 Le script python permet la connexion à une base CouchDB et l'envoi dans PostgreSql de toutes les tables métiers de SIRS Digues. Les geométries sont au format texte.


#sirs2postgis.py
 Saisies utilisateurs : 
- adresse du serveur sur lequel est installé SIRS
- adresse du serveur sur lequel est installé PostgreSql
- nom de la base SIRS
- nom de la base de données cible PostgreSql
- nom du schéma dans lequel les données vont être écrites. Si le schéma n'existe pas il est créé
- identifiant et mot de passe de l'utilisateur postgresSql

Les identifiants/mots de passe pour CouchDB sont ceux saisis à l'installation de sirs (geouser/geopw)

Dans PostgreSql, si le schéma et les tables existents, elles sont vidées avant d'être remplies. 

#f_creat_tabl_xml.sql

fonction de postgresql permettant de récupérer dans les xml de Sirs Digues la structure des objets (nom de la tables et champs). Les xml doivent être déposés dans le répertoire data de PostgreSql (ex : C:\Program Files\PostgreSQL\14\data).
Il faut adapter dans le code python selon le schéma dans lequel sera rangée cette fonction (ligne 85)

Il faut un xml par plugin. Pour le moment, seul le module général et le plugin dépendance ont été implentés dans la fonction
Pour récupérer les xml : https://github.com/Geomatys/SIRS-Digues-V2 / nom_du_plugin/model/www.ecore



