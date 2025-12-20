use crate::models::{CandidateStat, MatchBill1201, MatchBillItem1201, MatchResult1201, MatchedInvoiceItem};
use sqlx::PgPool;
use std::path::Path;
use bigdecimal::BigDecimal;

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

    tracing::debug!("开始构建批量插入语句, {} 条记录", results.len());
    let start_time = std::time::Instant::now();

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

    let build_elapsed = start_time.elapsed();
    tracing::debug!("SQL构建完成, 耗时: {:?}", build_elapsed);

    tracing::debug!("开始执行INSERT操作...");
    let execute_start = std::time::Instant::now();

    // 添加超时控制: 30秒
    let execute_result = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        query_builder.build().execute(pool)
    ).await;

    match execute_result {
        Ok(Ok(result)) => {
            let execute_elapsed = execute_start.elapsed();
            tracing::info!("✓ INSERT执行成功, 影响 {} 行, 耗时: {:?}", result.rows_affected(), execute_elapsed);
            Ok(())
        },
        Ok(Err(e)) => {
            let execute_elapsed = execute_start.elapsed();
            tracing::error!("✗ INSERT执行失败, 耗时: {:?}, 错误: {:?}", execute_elapsed, e);
            Err(e)
        },
        Err(_) => {
            tracing::error!("✗ INSERT操作超时 (>30秒)!");
            Err(sqlx::Error::PoolTimedOut)
        }
    }
}

/// 将 Option<BigDecimal> 转换为 CSV 字符串
fn option_to_csv(val: &Option<BigDecimal>) -> String {
    val.as_ref().map(|v| v.to_string()).unwrap_or_default()
}

/// 导出匹配结果到 CSV 文件（PostgreSQL COPY 兼容格式）
pub fn export_to_csv(
    results: &[MatchResult1201],
    output_path: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use csv::Writer;
    use std::fs::File;

    let file = File::create(output_path)?;
    let mut writer = Writer::from_writer(file);

    for result in results {
        writer.write_record(&[
            result.fbillid.to_string(),
            result.fbuyertaxno.clone(),
            result.fsalertaxno.clone(),
            result.fspbm.clone(),
            result.finvoiceid.to_string(),
            result.finvoiceitemid.to_string(),
            result.fnum.to_string(),
            result.fbillamount.to_string(),
            result.finvoiceamount.to_string(),
            result.fmatchamount.to_string(),
            option_to_csv(&result.fbillunitprice),
            option_to_csv(&result.fbillqty),
            option_to_csv(&result.finvoiceunitprice),
            option_to_csv(&result.finvoiceqty),
            result.fmatchtime.to_rfc3339(),
        ])?;
    }

    writer.flush()?;
    Ok(())
}
