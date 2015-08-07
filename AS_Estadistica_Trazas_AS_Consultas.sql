--Modificación número 2

--Y esto es una modificación al fichero para GIT


--Duración media y consultas por bases de datos
select Servidor, Fecha, avg(isnull(Duration,0)) as Duracion, count(*) as Cuantas
from dbo.SSAS_Monitor
group by Servidor, Fecha
order by Servidor, Fecha

--Cogemos las consultas más pesadas para poder ver situación de contadores en esos momentos exactos. 
select top 10 * 
from SSAS_Monitor
order by Duration desc;

--Ahora cogemos el número de cosultas y duración media por tramos de 5 minutos. 
with Rangos as (
	select Servidor, Fecha, cast((cast(starttime as numeric(14,7)) % 1)*86400/300 as int) as Rango, count(*) as Numero, avg(Duration) as Duracion
	from SSAS_Monitor
	group by Servidor, Fecha, cast((cast(starttime as numeric(14,7)) % 1)*86400/300 as int)
)
select Servidor, Fecha, right('00' + cast(cast(Rango*300/60/60 as int) as varchar(2)),2) + ':' + right('00' + cast(cast(Rango*300/60 as int) - cast(Rango*300/60/60 as int)*60 as varchar(2)), 2)
	+ ' - ' + right('00' + cast(cast((Rango*300+300)/60/60 as int) as varchar(2)),2) + ':' + right('00' + cast(cast((Rango*300+300)/60 as int) - cast((Rango*300+300)/60/60 as int)*60 as varchar(2)), 2), 
	Numero, Duracion
from Rangos
where Rango is not null
order by Servidor, Fecha, Rango;




--Consultas por bases de datos y duración según rangos de 3 segundos
with origen as (
select Servidor, Fecha, (cast(Duration/3000 as int) + 1) * 3 as Rango, count(*) as Cuantos, cast(sum(count(*)) over(partition by Servidor, Fecha) as numeric(14,2)) as Total
from dbo.SSAS_Monitor
group by Servidor, Fecha, cast(Duration/3000 as int)
)
select Servidor, Fecha, Rango, Cuantos, cast(Cuantos/Total*100 as numeric(14,2)) as Porc, cast(r.Desde as varchar(2)) + '-' + cast(r.Hasta as varchar(2)) as RangoMtoSIG
from Origen o
inner join tb_rangos r on o.Rango > r.Desde and o.Rango <= r.Hasta
where Rango is not null
order by Servidor, Fecha, Rango;


--Lo mismo pero en rangos predefinidos según mantenimiento SIG (menos de 3, entre 3 y 10, entre 10 y 30, más de 30). 
with origen as (
	select	Servidor, Fecha, (cast(isnull(Duration,0)/3000 as int) + 1) * 3 as Rango, count(*) as Cuantos, 
			cast(sum(count(*)) over(partition by Servidor, Fecha) as numeric(14,2)) as Total, 
			sum(isnull(Duration,0)) as DuracionTotalGrupo, 
			cast(sum(isnull(Serialization,0)) as numeric(14,2)) as DuracionSerializacionGrupo
	from dbo.SSAS_Monitor
	group by Servidor, Fecha, cast(isnull(Duration,0)/3000 as int)
)
select	Servidor, Fecha, RangoMtoSIG, sum(Cuantos) as Numero, 
		replace(cast(sum(Porc) as varchar(12)),'.', ',') as '%', 
		replace(cast(round(sum(DuracionSerializacionGrupo) / Sum(DuracionTotalGrupo) * 100, 2) as varchar(12)), '.', ',') as PorcSerializacion
from (	select Servidor, Fecha, Rango, Cuantos, 
		cast(Cuantos/Total*100 as numeric(14,2)) as Porc, 
		'Entre ' + RIGHT('00' + cast(r.Desde as varchar(2)), 2) + '-' + RIGHT('00' + cast(r.Hasta as varchar(2)), 2) as RangoMtoSIG, 
		DuracionTotalGrupo, 
		DuracionSerializacionGrupo
		from Origen o
		inner join tb_rangos r on o.Rango > r.Desde and o.Rango <= r.Hasta
		where Rango is not null
) t1
group by Servidor, Fecha, RangoMtoSIG;

