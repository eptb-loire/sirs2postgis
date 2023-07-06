-- FUNCTION: sirs.f_creation_tabl_xml(text, text)

-- DROP FUNCTION IF EXISTS sirs.f_creation_tabl_xml(text, text);

CREATE OR REPLACE FUNCTION sirs.f_creation_tabl_xml(
	se text,
	nomtable text)
    RETURNS TABLE(commande_crea text) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE monse alias for $1;
DECLARE matable alias for $2;
begin 
return QUERY 
execute
'
with monxml as(select xml::text  from 
				 unnest(
					xpath
					(    ''//eClassifiers''
						,XMLPARSE(DOCUMENT convert_from(pg_read_binary_file(''sirs_modele.xml''), ''UTF8''))
					)
				) AS myTempTable(xml)

				 union 
				  select xml::text  from 
				 unnest(
					xpath
					(    ''//eClassifiers''
						,XMLPARSE(DOCUMENT convert_from(pg_read_binary_file(''sirs_modele_dependance.xml''), ''UTF8''))
					)
				) AS myTempTable(xml)
					union 
				  select xml::text  from 
				 unnest(
					xpath
					(    ''//eClassifiers''
						,XMLPARSE(DOCUMENT convert_from(pg_read_binary_file(''sirs_modele_aot_cot.xml''), ''UTF8''))
					)
				) AS myTempTable(xml)			
				 union 
				  select xml::text  from 
				 unnest(
					xpath
					(    ''//eClassifiers''
						,XMLPARSE(DOCUMENT convert_from(pg_read_binary_file(''sirs_modele_reglementaire.xml''), ''UTF8''))
					)
				) AS myTempTable(xml)	
				union 
				  select xml::text  from 
				 unnest(
					xpath
					(    ''//eClassifiers''
						,XMLPARSE(DOCUMENT convert_from(pg_read_binary_file(''sirs_modele_vegetation.xml''), ''UTF8''))
					)
				) AS myTempTable(xml)	
				 )
,
--on sépare le classifier qui est un contenant portant le nom de la table 
--et les structuralefeature qui sont le contenu et qui contiennet les attributs
mesvaleurs as	 (
			 SELECT
					xml monclassifier,
					unnest(xpath(''//eStructuralFeatures'', xml::xml)) monstructuralfeature
				FROM monxml
			)
--on recupere les attribut de niveau 1 cest a dis ceux contenu dans le classifier globale

,attrniv1 as(
			select
			replace(split_part(split_part((monclassifier::text),'' '',4),''='',2)	,''"'','''')lestables,
			replace(split_part(split_part(monstructuralfeature::text,'' '',4),''='',2),''"'','''') lesattributs
			from mesvaleurs
			order by 1
				) 

, attrniv1_1 as (
			select 
					trim(BOTH ''>
						 '' FROM lestables ) as lestables, 
					lower(lesattributs) lesattributs from attrniv1
			)

--on fait un tableau des tables présentes en super type dont les attributs doivent être récupérés 
,tablost as(
			select distinct(
							replace(split_part(split_part(monclassifier::text,'' '',4),''='',2)	,''"'','''')
							)lestables,
				string_to_array(
							split_part(split_part(monclassifier::text,''eSuperTypes='',2),''>'',1),''#//'',''"'') tablost
			from mesvaleurs

			order by 1)

,st as(
		select lestables, replace(unnest(tablost),''"'','''') st from tablost
			)

--On recupere les attributs presents dans le premier supertype du classifier
	,attrniv2 as(
				select st.lestables, attrniv1.lesattributs
					from st inner join attrniv1 on trim(st.st)=trim(attrniv1.lestables)
)
--on recupere les attributs présents dans le supertype du supertype du classifier
,
 attrniv3 as (select st.lestables, attrniv2.lesattributs 
			   from st inner join attrniv2 
			   on trim(st.st) = trim(attrniv2.lestables) 
			   )
, attrniv4 as (select st.lestables, attrniv3.lesattributs 
			   from st inner join attrniv3
			   on trim(st.st) = trim(attrniv3.lestables) )

, attrniv5 as (select st.lestables, attrniv4.lesattributs 
			   from st inner join attrniv4
			   on trim(st.st) = trim(attrniv4.lestables)   )

,attrniv6 as (select st.lestables, attrniv5.lesattributs 
			   from st inner join attrniv5
			   on trim(st.st) = trim(attrniv5.lestables) ) 

, allattr as (			   
				select * from attrniv1_1 
	union 
				select * from attrniv2 
	union
				select * from attrniv3
	union
				select * from attrniv4 
	union
				select * from attrniv5 
	union
				select * from attrniv6)

select ''create table if not exists '||monse||'.''||lestables||''( _id text, ''||string_agg(lesattributs||'' text '','','') ||'')''
from allattr
where lestables = any (''{'||matable||'}'')
 group by lestables
'

;
END;
$BODY$;

ALTER FUNCTION sirs.f_creation_tabl_xml(text, text)
    OWNER TO salome;

COMMENT ON FUNCTION sirs.f_creation_tabl_xml(text, text)
    IS 'Juillet 2022, Etablissement public Loire (Emilie et Salomé)
permet des générer toutes les commandes de création de tables à partir : 
- du xml des modules généraux de sirs digue : https://github.com/Geomatys/SIRS-Digues-V2/blob/master/sirs-core/model/sirs.ecore 
- du xml du modules de dépendance https://github.com/Geomatys/SIRS-Digues-V2/blob/master/plugin-dependance/model/dependance.ecore
Rangement des xml : C:\Program Files\PostgreSQL\14\data';
