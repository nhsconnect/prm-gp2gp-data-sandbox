CREATE TABLE gp2gp_resent_deduplicated as (
	(SELECT * FROM gp2gp_emis_resent_mi)
	EXCEPT
	(SELECT * FROM gp2gp_mi))


CREATE TABLE gp2gp_resent_duplicate as (
  (SELECT * FROM gp2gp_emis_resent_mi)
  INTERSECT
  (SELECT * FROM gp2gp_mi))


-- identify files where duplicates occur in the RR record type
SELECT gp2gp_resent_duplicate.*, gp2gp_emis_resent_mi."$path" FROM
	gp2gp_resent_duplicate
	left join gp2gp_emis_resent_mi
	on gp2gp_resent_duplicate.col5 = gp2gp_emis_resent_mi.col5
	and gp2gp_resent_duplicate.col7 = gp2gp_emis_resent_mi.col7
	where gp2gp_resent_duplicate.col1 = 'RR'
	limit 10


-- count the number of duplicates in each file with the RR record type
select path, count(*) from
	(select gp2gp_emis_resent_mi."$path" as path from gp2gp_resent_duplicate
	left join gp2gp_emis_resent_mi
	on gp2gp_resent_duplicate.col5 = gp2gp_emis_resent_mi.col5
	and gp2gp_resent_duplicate.col7 = gp2gp_emis_resent_mi.col7
	where gp2gp_resent_duplicate.col1 = 'RR')
	group by path


-- count the total number of all RR records in each file
select path, count(*) from
	(select "$path" as path from gp2gp_emis_resent_mi where col1 = 'RR')
	group by path
