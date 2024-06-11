select distinct
ap.dtInput, ap.Id,
chan1.IdChannel,	--канал с которой было первое обращение
isnull(max_delay,0) max_delay,
isnull(isnull(isnull(Reg_region,Liv_region),Work_region), 0)  region,
cl.Sex, vec.Age,  vec.Income, num_order,  vec.Loan_amount,
(convert (float,  vec.Loan_amount+isnull(abs(costs),0))) as amountordersr, --запрашиваемая сумма + ОСЗ
case when Income = 0 then 0 else (convert (float,  vec.Loan_amount+isnull(abs(costs),0)))/convert(float,CreditPeriod*Income) end PDN, --пдн по лимитам
case when isnull(ap.linsurAmount,0)=0 and  isnull(ap.PInsurAmount,0)=0 then 0	 --нет страховок
        when isnull(ap.linsurAmount,0)>0 and  isnull(ap.PInsurAmount,0)=0 then 1 --есть страховка жизни
        when isnull(ap.linsurAmount,0)=0 and  isnull(ap.PInsurAmount,0)>0 then 1 --если есть хоть одна страховка, то 1
        when isnull(ap.linsurAmount,0)>0 and  isnull(ap.PInsurAmount,0)>0 then 2 --если есть обе, то 2
        else '' end insurance, --страховка
isnull(costs, 0) costs, --задолжность
case when isnull(vec.Loan_amount, 0)=0 then 0
else isnull(abs(costs),0) /isnull(vec.Loan_amount, 1) end cost_on_la, --отношение задолжности к запрашиваемой сумме
isnull(bki.Days_since_last_credit, -1) nbki_Days_since_last_credit, isnull(bki.Nb_active_microcredits, -1) Nb_active_microcredits, isnull(bki.Nb_active_mortgages, -1) Nb_active_mortgages, isnull(score,0) scoreRetailPersonal
 from  Billing..Applications ap
inner join Billing..Client cl on cl.id=ap.ClientId
inner join DMS..Input_vector_data vec on ap.id=vec.AppId
inner join Billing..Organization o on ap.OrganizationiD=o.id
inner join Billing..SPRAV_Channel ch on ch.id=o.IdChannel
inner join (select AppId, ROW_NUMBER()over(partition by ClientId order by [Дата выдача]) as num_order from RISK_REPORT..riskmetrics) n on ap.Id=n.AppId
left join (select Clientid,dtinsert,min(Costs) as Costs from Billing.dbo.LogBalance group by ClientId,dtinsert) as bclt  on convert(datetime,bclt.dtinsert)=convert(date,ap.dtinput) and ap.clientid=bclt.clientid
left join (
select  distinct app.id, COUNT(1)over(partition by app.Id) as cnt_delay from Billing..DepthDelay d left join Billing..Applications app on d.Clientid=app.clientid and cast(d.dtInsert as date)<=cast(app.dtInput as date)
where d.[Day]=1 ) h on h.id=ap.id
left join (select distinct app.id, max(Day)over(partition by app.id) as max_delay from Billing..DepthDelay d left join Billing..Applications app on d.Clientid=app.clientid and cast(d.dtInsert as date)<=cast(app.dtInput as date)
where app.clientid  is not null) hi on ap.Id=hi.Id
left join
(select  nbki.appid, Total_overdue_amount, nbki.Total_active_accounts, nbki.Days_since_last_credit,  nbki.Nb_delays_60_90_ever, nbki.Max_overdue, nbki.Nb_active_consumer_credit, nbki.Nb_active_microcredits, nbki.Nb_active_mortgages, score
from  NBKI.dbo.NBKI_Response_V2 nbki
left join nbki..NBKI_RetailScorePV20_V2 rs
on nbki.AppId=rs.appId) bki on bki.appid=ap.id
left join (select chan.ClientId, IdChannel from (select ClientId, IdChannel, ROW_NUMBER()over (partition by ap.ClientId order by ap.dtInput asc) id_ch_first
from Billing..Applications ap
inner join Billing..Organization o on ap.OrganizationiD=o.id
inner join Billing..SPRAV_Channel ch on ch.id=o.IdChannel) chan where id_ch_first=1) chan1 on chan1.ClientId=ap.clientid
--добавляем фильтр по кешу, хотя это должно быть в стратегии и рандом заявку тянем
where ap.id=2098341
