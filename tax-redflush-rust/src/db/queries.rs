use crate::models::{CandidateStat, MatchBill1201, MatchBillItem1201, MatchResult1201, MatchedInvoiceItem};
use sqlx::PgPool;

/// 查询单据主表
pub async fn get_bill(
    pool: &PgPool,
    bill_id: i64,
) -> Result<Option<MatchBill1201>, sqlx::Error> {
    sqlx::query_as::<_, MatchBill1201>(
        r#"
        SELECT fid, fbuyertaxno, fsalertaxno
        FROM t_sim_match_bill_1201
        WHERE fid = $1
        "#
    )
    .bind(bill_id)
    .fetch_optional(pool)
    .await
}

/// 查询单据明细列表
pub async fn list_bill_items(
    pool: &PgPool,
    bill_id: i64,
) -> Result<Vec<MatchBillItem1201>, sqlx::Error> {
    sqlx::query_as::<_, MatchBillItem1201>(
        r#"
        SELECT fid, fentryid, fspbm, famount, fnum, funitprice
        FROM t_sim_match_bill_item_1201
        WHERE fid = $1
        "#
    )
    .bind(bill_id)
    .fetch_all(pool)
    .await
}

/// 统计候选发票数量和总金额
pub async fn stat_for_product(
    pool: &PgPool,
    buyer_tax_no: &str,
    seller_tax_no: &str,
    product_code: &str,
) -> Result<CandidateStat, sqlx::Error> {
    sqlx::query_as::<_, CandidateStat>(
        r#"
        SELECT count(*) as cnt,
               coalesce(sum(vii.famount), 0) as sum_amount
        FROM t_sim_vatinvoice_item_1201 vii
        INNER JOIN t_sim_vatinvoice_1201 vi ON vi.fid = vii.fid
        WHERE vii.fspbm = $1
          AND vi.fbuyertaxno = $2
          AND vi.fsalertaxno = $3
          AND vi.ftotalamount > 0
        "#
    )
    .bind(product_code)
    .bind(buyer_tax_no)
    .bind(seller_tax_no)
    .fetch_one(pool)
    .await
}

/// 查询候选发票 (按金额降序 - 大金额优先填充)
pub async fn match_by_tax_and_product(
    pool: &PgPool,
    buyer_tax_no: &str,
    seller_tax_no: &str,
    product_code: &str,
) -> Result<Vec<MatchedInvoiceItem>, sqlx::Error> {
    sqlx::query_as::<_, MatchedInvoiceItem>(
        r#"
        SELECT vii.fid as invoice_id,
               vii.fentryid as item_id,
               vii.fspbm as product_code,
               vii.fnum as quantity,
               vii.famount as amount,
               vii.funitprice as unit_price
        FROM t_sim_vatinvoice_item_1201 vii
        INNER JOIN t_sim_vatinvoice_1201 vi ON vi.fid = vii.fid
        WHERE vii.fspbm = $1
          AND vi.fbuyertaxno = $2
          AND vi.fsalertaxno = $3
          AND vi.ftotalamount > 0
        ORDER BY vii.famount DESC
        "#
    )
    .bind(product_code)
    .bind(buyer_tax_no)
    .bind(seller_tax_no)
    .fetch_all(pool)
    .await
}

/// 从指定发票ID中查询 (按金额升序 - 复用时小金额优先)
pub async fn match_on_invoices(
    pool: &PgPool,
    buyer_tax_no: &str,
    seller_tax_no: &str,
    product_code: &str,
    invoice_ids: &[i64],
) -> Result<Vec<MatchedInvoiceItem>, sqlx::Error> {
    sqlx::query_as::<_, MatchedInvoiceItem>(
        r#"
        SELECT vii.fid as invoice_id,
               vii.fentryid as item_id,
               vii.fspbm as product_code,
               vii.fnum as quantity,
               vii.famount as amount,
               vii.funitprice as unit_price
        FROM t_sim_vatinvoice_item_1201 vii
        INNER JOIN t_sim_vatinvoice_1201 vi ON vi.fid = vii.fid
        WHERE vii.fspbm = $1
          AND vi.fbuyertaxno = $2
          AND vi.fsalertaxno = $3
          AND vi.ftotalamount > 0
          AND vii.fid = ANY($4)
        ORDER BY vii.famount ASC
        "#
    )
    .bind(product_code)
    .bind(buyer_tax_no)
    .bind(seller_tax_no)
    .bind(invoice_ids)
    .fetch_all(pool)
    .await
}

/// 批量插入匹配结果
pub async fn insert_batch(
    pool: &PgPool,
    results: &[MatchResult1201],
) -> Result<(), sqlx::Error> {
    if results.is_empty() {
        return Ok(());
    }

    // 构建批量插入语句
    let mut query_builder = sqlx::QueryBuilder::new(
        "INSERT INTO t_sim_match_result_1201 (
            fbillid, fbuyertaxno, fsalertaxno, fspbm,
            finvoiceid, finvoiceitemid, fnum,
            fbillamount, finvoiceamount, fmatchamount,
            fbillunitprice, fbillqty, finvoiceunitprice, finvoiceqty,
            fmatchtime
        ) "
    );

    query_builder.push_values(results, |mut b, result| {
        b.push_bind(result.fbillid)
            .push_bind(&result.fbuyertaxno)
            .push_bind(&result.fsalertaxno)
            .push_bind(&result.fspbm)
            .push_bind(result.finvoiceid)
            .push_bind(result.finvoiceitemid)
            .push_bind(result.fnum.clone())
            .push_bind(result.fbillamount.clone())
            .push_bind(result.finvoiceamount.clone())
            .push_bind(result.fmatchamount.clone())
            .push_bind(result.fbillunitprice.clone())
            .push_bind(result.fbillqty.clone())
            .push_bind(result.finvoiceunitprice.clone())
            .push_bind(result.finvoiceqty.clone())
            .push_bind(result.fmatchtime);
    });

    query_builder.build().execute(pool).await?;
    Ok(())
}
