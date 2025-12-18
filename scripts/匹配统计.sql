with match_orders as (
    select 
        re.finvoiceitemid,
        re.fmatchamount,
        re.fid as match_id,
        row_number() over(
            partition by re.finvoiceitemid 
            order by re.fmatchtime, re.fid  -- 按红冲时间排序，如果没有时间字段可以用其他字段
        ) as match_order
    from t_sim_match_result_1201 re
)
select row_number() over() 序号,
re.fspbm 待红冲SKU,
re.finvoiceid 该sku红冲对应蓝票id,
inv.finvoiceno 该sku红冲对应蓝票的发票号码,
inv.fissuetime 该sku红冲对应蓝票的开票日期,
-- re.finvoiceitemid,
invitem.fseq 该sku红冲对应蓝票的发票行号,
invitem.famount 该SKU红冲对应蓝票行的剩余可红冲金额,
invitem.fredprice 该SKU红冲对应蓝票行的可红冲单价,
re.fmatchamount 本次红冲扣除的红冲金额（正数）,
case when invitem.funitprice = 0 then 0 else re.fmatchamount / invitem.funitprice end 本次红冲扣除SKU数量,
invitem.famount - sum(re.fmatchamount) over(
        partition by re.finvoiceitemid 
        order by mo.match_order
        rows between unbounded preceding and current row
    ) as 扣除本次红冲后对应蓝票行的剩余可红冲金额,
case when wholeRow.rowmatchamount = invitem.famount then '行全额红冲' else '行部分红冲' end 是否属于整行红冲
from t_sim_match_result_1201 re
join t_sim_vatinvoice_1201 inv on re.finvoiceid = inv.fid 
join t_sim_vatinvoice_item_1201 invitem on re.finvoiceitemid = invitem.fentryid
join (select finvoiceitemid, sum(fmatchamount) rowmatchamount
    from t_sim_match_result_1201
    group by finvoiceitemid
) wholeRow on wholeRow.finvoiceitemid = invitem.fentryid
join match_orders mo on re.fid = mo.match_id
order by re.finvoiceid ,re.finvoiceitemid;

with re as (
	select fspbm,
	sum(case when finvoiceqty = 0 then 0 else fmatchamount / finvoiceqty end) finvoiceqty,
	sum(fmatchamount) fmatchamount,
	count(finvoiceitemid) finvoiceitemid
	from t_sim_match_result_1201 tsmr 
	group by fspbm
)
select re.fspbm,
billitem.famount 待红冲sku总金额,
billitem.fnum 待红冲sku总数量,
case when billitem.fnum = 0 then 0 else billitem.famount / billitem.fnum end 待红冲sku平均单价,
re.finvoiceqty 该SKU红冲扣除蓝票的总数量,
re.fmatchamount 该SKU红冲扣除蓝票的总金额,
case when billitem.fnum = 0 then 0 else re.fmatchamount / (billitem.famount / billitem.fnum) end 计算出来的数量,
finvoiceitemid SKU红冲扣除蓝票的总行数
from t_sim_match_bill_item_1201 billitem
join re on re.fspbm = billitem.fspbm;


with invitem as(
	select fid,
	count(1) totalRow,
	sum(famount) famount
	from t_sim_vatinvoice_item_1201
	group by fid
)
select row_number() over() 序号,
re.finvoiceid 红冲计算结果对应的蓝票fid,
min(inv.finvoiceno)  红冲计算结果对应的蓝票发票号码,
min(inv.fissuetime) 红冲计算结果对应的蓝票开票日期,
min(invitem.totalrow) 红冲计算结果对应蓝票的总行数,
min(invitem.famount) 红冲计算结果对应蓝票的总金额,
count(1) 本次红冲结果运算扣除的蓝票总行数,
sum(re.fmatchamount) 本次红冲结果运算扣除的蓝票总金额,
count(1)::decimal / nullif(min(invitem.totalrow), 0) 整张红冲的行数比例
from t_sim_match_result_1201 re
join t_sim_vatinvoice_1201 inv on re.finvoiceid = inv.fid
join invitem on re.finvoiceid = invitem.fid
group by re.finvoiceid;