select distinct
ord.AppId,
inp.Age,
app.InitialFee, -- первоначальный взнос
app.AmountPurchaseOriginal, --- стоимость товара
cl.sex,
cl.educationid,---[dbo].[SPRAV_Education]
ad.averagemonthlyincome, -- средний доход

--'Вызов НБКИ-->>' [Сервис 1],

bki_nbki.Total_overdue_amount,
bki_nbki.Total_installment_amount,
bki_nbki.Recent_inquiries,
bki_nbki.Nb_delays_60_90_ever,
bki_nbki.Max_overdue,
bki_nbki.RCC_credit_limit,
isnull(score.score, bki_nbki.scoreRetailPersonal) score

From dbo.applications app (nolock)
left join dbo.client cl (nolock) on cl.id=app.clientid
left join dbo.Organization org (nolock) on (org.id=app.organizationid)
left join [dbo].[Client_AdditionalInfo] as ad with(nolock) on (ad.ClientId=app.ClientId)
left join [dbo].[Client_work]  as cl_w  with(nolock) on (cl_w.ClientId=app.ClientId)
left join dms..input_vector_data inp (nolock) on (inp.appid=app.id)
left join dms..[Input_vector_bki] bki_nbki (nolock) on (bki_nbki.appid=app.id)
left join Billing..Orders ord on ord.AppId=app.Id
left join nbki..NBKI_RetailScorePV20_V2 score   on inp.ClientId=score.clientId and cast(inp.Datenter as date)=cast(score.dtInsert as date)  -- on app.id=score.appid
left join NBKI.[dbo].[NBKI_Response_V2] res1 on (res1.appid=app.Id)


where r.client_type='new'
and cl.NonResident=0
and org.IsDisableMegafon=0
and Channel not in ('Яндекс POS')
and org.CategoryGoodsId  not in (1,2,3,28,41,57)
and cl.IsTest=0
and  ord.Cancellation!=1
--and cast(dtInput as date) between cast('2022-10-01' as date) and cast('2023-12-31' as date)